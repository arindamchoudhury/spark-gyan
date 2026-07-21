import requests
from bs4 import BeautifulSoup
import re
import time

def extract_jira_notes(jira_url):
    """Fetches and extracts the raw text from an Apache JIRA release notes page."""
    print(f"  -> Following JIRA redirect: {jira_url}")
    try:
        req = requests.get(jira_url)
        soup = BeautifulSoup(req.text, 'html.parser')
        
        content = soup.find('textarea') 
        if content:
            return content.get_text(strip=True)
            
        list_items = soup.find_all('li')
        if list_items:
            return "\n".join([li.get_text(separator=' ', strip=True) for li in list_items if 'SPARK-' in li.text])
            
        return soup.get_text(separator='\n', strip=True)
    except Exception as e:
        return f"  [Failed to fetch JIRA notes: {e}]"

def get_version_tuple(url):
    """Extracts the version numbers from the URL for proper mathematical sorting."""
    # Find the version part of the string (e.g., "4-2-0" or "4.1.1" or "0-3")
    match = re.search(r'spark-release-(.*?)\.html', url)
    if not match:
        return (0,)
    
    version_str = match.group(1)
    # Standardize separator to dot, then split
    version_str = version_str.replace('-', '.')
    
    # Convert ["4", "2", "0"] into (4, 2, 0)
    try:
        return tuple(int(part) for part in version_str.split('.'))
    except ValueError:
        return (0,)

def crawl_spark_releases():
    base_url = "https://spark.apache.org"
    releases_url = f"{base_url}/releases/"
    output_file = "spark_all_changelogs.txt"

    print("Fetching list of releases...")
    response = requests.get(releases_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    release_links = []
    for link in soup.find_all('a', href=re.compile(r'spark-release-.*\.html')):
        href = link.get('href')
        if not href.startswith('http'):
            href = f"{base_url}{href}" if href.startswith('/') else f"{releases_url}{href}"
        release_links.append(href)

    # === NEW: Sort mathematically using the get_version_tuple key ===
    unique_links = list(set(release_links))
    release_links = sorted(unique_links, key=get_version_tuple, reverse=True)

    print(f"Found {len(release_links)} release notes. Downloading in proper descending order...")

    with open(output_file, 'w', encoding='utf-8') as f:
        for url in release_links:
            try:
                print(f"Processing {url}...")
                req = requests.get(url)
                page_soup = BeautifulSoup(req.text, 'html.parser')
                
                content_container = page_soup.find('div', class_='col-md-9') or page_soup.find('article') or page_soup.find('body')
                
                if content_container:
                    for nav in content_container.find_all(['nav', 'header', 'footer', 'div'], class_=['col-md-3']):
                        nav.decompose()
                    
                    text = content_container.get_text(separator='\n', strip=True)
                    
                    f.write(f"{'='*80}\n")
                    f.write(f"RELEASE: {url.split('/')[-1].replace('.html', '')}\n")
                    f.write(f"SOURCE: {url}\n")
                    f.write(f"{'='*80}\n\n")
                    f.write(text)
                    f.write("\n\n")

                    jira_link = content_container.find('a', href=re.compile(r'issues\.apache\.org/jira/.*ReleaseNote\.jspa'))
                    if jira_link:
                        jira_text = extract_jira_notes(jira_link['href'])
                        f.write("\n--- JIRA RELEASE NOTES ---\n")
                        f.write(jira_text)
                        f.write("\n\n")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Failed to process {url}: {e}")

    print(f"\nDone! All changelogs successfully aggregated into {output_file}")

if __name__ == "__main__":
    crawl_spark_releases()