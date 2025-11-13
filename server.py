# server.py
# type: ignore
from fastmcp import FastMCP
import httpx
from bs4 import BeautifulSoup
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional
import sys

mcp = FastMCP("marathon-crawler")

# 간단한 메모리 캐시
_cache = {
    'data': None,
    'timestamp': None,
    'ttl': 3600  # 1시간 캐시
}

async def fetch_detail(client: httpx.AsyncClient, detail_url: str, base_domain: str) -> Optional[dict]:
    """정적 단일 마라톤 상세 정보 가져오기"""
    try:
        full_url = base_domain + detail_url if not detail_url.startswith('http') else detail_url
        response = await client.get(full_url, timeout= 15.0)
        response.raise_for_status()

        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        
        if script_tag:
            json_data = json.loads(script_tag.string)
            race_detail = json_data.get('props', {}).get('pageProps', {}).get('raceDetail', {})
            
            if race_detail:
                return {
                    '마라톤명': race_detail.get('raceName', ''),
                    '트랙': race_detail.get('raceTypeList', '').split(',') if race_detail.get('raceTypeList') else [],
                    '지역': race_detail.get('region', ''),
                    '장소': race_detail.get('place', ''),
                    '날짜': race_detail.get('raceDate', ''),
                    '집결시간': race_detail.get('raceStart', ''),
                    '접수기간': {
                        '시작일': race_detail.get('applicationStartDate', ''),
                        '종료일': race_detail.get('applicationEndDate', '')
                    },
                    '문의처': {
                        '이메일': race_detail.get('email', ''),
                        '전화번호': race_detail.get('phone', '')
                    },
                    '주최': race_detail.get('host', ''),
                    '홈페이지': race_detail.get('homepageUrl', ''),
                    '소개': race_detail.get('intro', ''),
                    '상세URL': detail_url
                }
    except Exception as e:
        print(f"Error fetching {detail_url}: {e}", file=sys.stderr)
        return None

def is_accepting_applications(marathon: dict) -> bool:
    """접수 가능 여부 확인"""
    try:
        end_date_str = marathon.get('접수기간', {}).get('종료일', '')
        if not end_date_str:
            return False
        
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        return end_date >= today
    except:
        return False

async def crawl_marathons_fast(base_url: str, base_domain: str, max_concurrent: int = 10) -> list:
    """병렬 처리로 빠르게 크롤링"""
    all_marathons = []
    
    async with httpx.AsyncClient() as client:
        try:
            # 1. 목록 페이지 HTML 정적 요청
            response = await client.get(base_url, timeout=30.0)
            response.raise_for_status()
            html = response.text
            
            soup = BeautifulSoup(html, 'html.parser')
            marathon_links = soup.find_all('a', class_='MuiLink-root')
            
            detail_urls = []
            for link in marathon_links:
                href = link.get('href', '')
                if href and '/raceDetail/' in href and href not in detail_urls:
                    detail_urls.append(href)
            
            if not detail_urls:
                print("경고: 상세 페이지 링크를 찾지 못했습니다. (사이트 구조 변경 가능성)", file=sys.stderr)
                return []
            
            # 2. 병렬로 상세 페이지 크롤링
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def fetch_with_semaphore(url):
                async with semaphore:
                    result = await fetch_detail(client, url, base_domain)
                    return result
            
            tasks = [fetch_with_semaphore(url) for url in detail_urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            all_marathons = [r for r in results if r and not isinstance(r, Exception)]
            
        except httpx.HTTPStatusError as e:
            print(f"HTTP 오류 발생 : {e.response.status_code} - {e.request.url}", file=sys.stderr)
        except Exception as e:
            print(f"크롤링 중 알 수 없는 오류 발생 : {e}", file=sys.stderr)
    
    return all_marathons

def is_cache_valid() -> bool:
    """캐시가 유효한지 확인"""
    if _cache['data'] is None or _cache['timestamp'] is None:
        return False
    
    elapsed = datetime.now() - _cache['timestamp']
    return elapsed.total_seconds() < _cache['ttl']

@mcp.tool()
async def search_marathons(
    region: str = "",
    date: str = "",
    only_accepting: bool = False,
    use_cache: bool = True
) -> str:
    """
    한국의 마라톤 대회 정보를 검색합니다.
    
    Args:
        region: 지역 필터 (예: '서울', '경기', '부산')
        date: 날짜 필터 (예: '2025-11', '2025-11-15')
        only_accepting: 접수 가능한 대회만 반환
        use_cache: 캐시 사용 여부
    
    Returns:
        JSON 형식의 마라톤 정보
        {
            "success": true,
            "total": 5,
            "filters": {...},
            "marathons": [
                {
                    "마라톤명": "...",
                    "트랙": ["10km", "5km"],
                    "지역": "서울",
                    "장소": "...",
                    "날짜": "2025-11-20",
                    "집결시간": "07:00",
                    "접수기간": {"시작일": "...", "종료일": "..."},
                    "문의처": {"이메일": "...", "전화번호": "..."},
                    "주최": "...",
                    "홈페이지": "...",
                    "소개": "...",
                    "상세URL": "...",
                    "접수가능여부": true
                }
            ]
        }
    """
    
    # 캐시 확인
    if use_cache and is_cache_valid():
        print("캐시된 데이터를 사용", file=sys.stderr)
        results = _cache['data']
    else:
        print("새로운 데이터 fetching", file=sys.stderr)
        results = await crawl_marathons_fast(
            "https://marathongo.co.kr/races",
            "https://marathongo.co.kr",
            max_concurrent=10
        )
        if results:
            _cache['data'] = results
            _cache['timestamp'] = datetime.now()
            print(f"데이터 {len(results)}개 로드 및 캐시 저장", file=sys.stderr)
        else:
            return json.dumps({
                "success": False,
                "total": 0,
                "error": "데이터를 가져올 수 없습니다",
                "marathons": []
            }, ensure_ascii=False)
        
    # 필터링 적용
    filtered = results
    
    if region:
        filtered = [m for m in filtered if region in m.get('지역', '')]
    
    if date:
        filtered = [m for m in filtered if date in m.get('날짜', '')]
    
    if only_accepting:
        filtered = [m for m in filtered if is_accepting_applications(m)]
    
    # 날짜순 정렬
    filtered.sort(key=lambda x: x.get('날짜', '9999-99-99'))
    
    # 접수가능여부 필드 추가
    marathons_with_status = []
    for marathon in filtered:
        m = marathon.copy()
        m['접수가능여부'] = is_accepting_applications(marathon)
        marathons_with_status.append(m)
    
    # JSON 반환
    return json.dumps({
        "success": len(marathons_with_status) > 0,
        "total": len(marathons_with_status),
        "filters": {
            "region": region if region else None,
            "date": date if date else None,
            "only_accepting": only_accepting
        },
        "marathons": marathons_with_status
    }, ensure_ascii=False, indent=2)
    

@mcp.tool()
async def clear_cache() -> str:
    """
    마라톤 데이터 캐시를 삭제합니다.
    최신 정보가 필요할 때 사용하세요.
    
    Returns:
        JSON 결과
    """
    _cache['data'] = None
    _cache['timestamp'] = None
    
    return json.dumps({
        "success": True,
        "message": "캐시가 삭제되었습니다"
    }, ensure_ascii=False)


if __name__ == "__main__":
    mcp.run()