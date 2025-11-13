# marathon_server_agent.py
# AI Agent 전용 - JSON만 반환
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
        response = await client.get(full_url, timeout=15.0)
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
                print("경고: 상세 페이지 링크를 찾지 못했습니다.", file=sys.stderr)
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
            print(f"HTTP 오류: {e.response.status_code}", file=sys.stderr)
        except Exception as e:
            print(f"크롤링 오류: {e}", file=sys.stderr)
    
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
        results = _cache['data']
    else:
        results = await crawl_marathons_fast(
            "https://marathongo.co.kr/races",
            "https://marathongo.co.kr",
            max_concurrent=10
        )
        if results:
            _cache['data'] = results
            _cache['timestamp'] = datetime.now()
        else:
            return json.dumps({
                "success": False,
                "total": 0,
                "error": "데이터를 가져올 수 없습니다",
                "marathons": []
            }, ensure_ascii=False)
    
    # 필터링
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
async def get_marathon_by_name(name: str, use_cache: bool = True) -> str:
    """
    마라톤 이름으로 검색합니다 (부분 일치).
    
    Args:
        name: 검색할 마라톤 이름
        use_cache: 캐시 사용 여부
    
    Returns:
        JSON 형식의 마라톤 정보
    """
    
    # 캐시 확인
    if use_cache and is_cache_valid():
        results = _cache['data']
    else:
        results = await crawl_marathons_fast(
            "https://marathongo.co.kr/races",
            "https://marathongo.co.kr",
            max_concurrent=10
        )
        if results:
            _cache['data'] = results
            _cache['timestamp'] = datetime.now()
    
    # 이름 검색 (대소문자 무시)
    matched = [m for m in results if name.lower() in m.get('마라톤명', '').lower()]
    
    if not matched:
        return json.dumps({
            "success": False,
            "message": f"'{name}'을 찾을 수 없습니다",
            "marathon": None
        }, ensure_ascii=False)
    
    # 첫 번째 결과 반환
    marathon = matched[0].copy()
    marathon['접수가능여부'] = is_accepting_applications(marathon)
    
    result = {
        "success": True,
        "total_matches": len(matched),
        "marathon": marathon
    }
    
    # 여러 개 매칭되면 목록 추가
    if len(matched) > 1:
        result['other_matches'] = [
            {"마라톤명": m.get('마라톤명'), "날짜": m.get('날짜')}
            for m in matched[1:]
        ]
    
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_upcoming_marathons(days: int = 30, use_cache: bool = True) -> str:
    """
    앞으로 N일 이내의 마라톤을 조회합니다.
    
    Args:
        days: 조회 기간 (일)
        use_cache: 캐시 사용 여부
    
    Returns:
        JSON 형식의 마라톤 목록 (D-day 포함)
    """
    
    # 캐시 확인
    if use_cache and is_cache_valid():
        results = _cache['data']
    else:
        results = await crawl_marathons_fast(
            "https://marathongo.co.kr/races",
            "https://marathongo.co.kr",
            max_concurrent=10
        )
        if results:
            _cache['data'] = results
            _cache['timestamp'] = datetime.now()
    
    # 날짜 필터링
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = today + timedelta(days=days)
    
    upcoming = []
    for marathon in results:
        try:
            race_date_str = marathon.get('날짜', '')
            if race_date_str:
                race_date = datetime.strptime(race_date_str, '%Y-%m-%d')
                if today <= race_date <= end_date:
                    m = marathon.copy()
                    m['접수가능여부'] = is_accepting_applications(marathon)
                    m['D-day'] = (race_date - today).days
                    upcoming.append(m)
        except:
            continue
    
    # D-day 순 정렬
    upcoming.sort(key=lambda x: x.get('D-day', 999))
    
    return json.dumps({
        "success": len(upcoming) > 0,
        "total": len(upcoming),
        "period_days": days,
        "marathons": upcoming
    }, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_marathons_by_track(track: str, use_cache: bool = True) -> str:
    """
    특정 트랙 종류의 마라톤을 검색합니다.
    
    Args:
        track: 트랙 종류 (예: '5km', '10km', '하프', '풀코스')
        use_cache: 캐시 사용 여부
    
    Returns:
        JSON 형식의 마라톤 목록
    """
    
    # 캐시 확인
    if use_cache and is_cache_valid():
        results = _cache['data']
    else:
        results = await crawl_marathons_fast(
            "https://marathongo.co.kr/races",
            "https://marathongo.co.kr",
            max_concurrent=10
        )
        if results:
            _cache['data'] = results
            _cache['timestamp'] = datetime.now()
    
    # 트랙 필터링
    matched = []
    for marathon in results:
        tracks = marathon.get('트랙', [])
        if any(track.lower() in t.lower() for t in tracks):
            m = marathon.copy()
            m['접수가능여부'] = is_accepting_applications(marathon)
            matched.append(m)
    
    # 날짜순 정렬
    matched.sort(key=lambda x: x.get('날짜', '9999-99-99'))
    
    return json.dumps({
        "success": len(matched) > 0,
        "total": len(matched),
        "track_filter": track,
        "marathons": matched
    }, ensure_ascii=False, indent=2)


@mcp.tool()
async def clear_cache() -> str:
    """
    캐시를 삭제합니다.
    
    Returns:
        JSON 형식의 결과
    """
    _cache['data'] = None
    _cache['timestamp'] = None
    
    return json.dumps({
        "success": True,
        "message": "캐시가 삭제되었습니다"
    }, ensure_ascii=False)


if __name__ == "__main__":
    mcp.run()