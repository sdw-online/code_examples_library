import pandas as pd
import urllib.request

def fetch_robots_txt(url):
    """Fetches the robots.txt file from the specified URL."""
    robots_url = urllib.parse.urljoin(url, '/robots.txt')
    request = urllib.request.Request(robots_url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        response = urllib.request.urlopen(request)
        content = response.read().decode(response.headers.get_content_charset() or 'utf-8')
        print(f"✅ Successfully fetched robots.txt from {url} - 1/3 🟠")
        return content
    except Exception as e:
        print(f"❌ Failed to fetch robots.txt from {url}: {e}")
        return None

def parse_robots_txt(robots_txt):
    """Parses the robots.txt content and returns sitemaps + directives."""
    sitemaps = []
    directives = []
    for line in robots_txt.splitlines():
        if line.startswith('Sitemap:'):
            sitemaps.append(line.split(':', 1)[1].strip())
        elif line and not line.startswith('#'):
            parts = line.split(':', 1)
            if len(parts) == 2:
                directives.append(parts)
    print("✅ Parsed robots.txt content successfully - 2/3 🟡")
    return sitemaps, directives

def convert_robots_to_dataframe(directives):
    """Converts robots.txt directives to a pandas DataFrame."""
    df = pd.DataFrame(directives, columns=['Directive', 'Parameter'])
    print("✅ Converted robots.txt directives to dataframe - 3/3 🟢")
    return df



# Fetch and parse robots.txt content
robots_content = fetch_robots_txt("https://twtd.co.uk/")

if robots_content:
    sitemaps, directives = parse_robots_txt(robots_content)
    df_directives = convert_robots_to_dataframe(directives)
    
    # Check if sitemaps were found and display them
    if sitemaps:
        print("\nSitemaps found in robots.txt:")
        for sitemap in sitemaps:
            print(f"- {sitemap}")
    else:
        print("\nNo sitemaps found in robots.txt.")
    
    # Check if directives were found and display the first 10
    if not df_directives.empty:
        print("\nDirectives from robots.txt:")
        print(df_directives.head(10))
    else:
        print("\nNo directives found in robots.txt.")


