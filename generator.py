import json
import random
import time
import argparse
import string
import sys
from curl_cffi import requests
from tqdm import tqdm

class HushraGenerator:
    def __init__(self, output_file="uuids.txt", proxy_list=None):
        self.url = "https://api.hushra.me/api/v1/auth/registration"
        self.output_file = output_file
        self.proxies = proxy_list
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Mobile/15E148 Safari/604.1"
        ]

    def generate_username(self, length=8):
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choice(chars) for _ in range(length))

    def get_random_headers(self):
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://hushra.me",
            "Referer": "https://hushra.me/login",
            "User-Agent": random.choice(self.user_agents),
            "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="122", "Chromium";v="122"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }

    def create_account(self, username):
        headers = self.get_random_headers()
        payload = {"username": username}
        
        proxy = None
        if self.proxies:
            p = random.choice(self.proxies)
            proxy = {"http": p, "https": p}

        try:
            # impersonate="chrome110" helps bypass Cloudflare/TLS fingerprinting
            response = requests.post(
                self.url, 
                json=payload, 
                headers=headers, 
                impersonate="chrome110",
                proxies=proxy,
                timeout=15
            )
            
            if response.status_code == 200 or response.status_code == 201:
                return response.json()
            else:
                print(f"\n[!] Error for {username}: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"\n[!] Request failed: {e}")
            return None

    def save_account(self, uuid):
        try:
            with open(self.output_file, 'a') as f:
                f.write(f"{uuid}\n")
        except Exception as e:
            print(f"\n[!] Error saving UUID: {e}")

    def run(self, count, min_delay=5, max_delay=15):
        print(f"[*] Starting generation of {count} accounts...")
        successful = 0
        
        for i in tqdm(range(count), desc="Generating Accounts"):
            username = self.generate_username()
            result = self.create_account(username)
            
            if result and 'uuid' in result:
                # Based on research, response is likely {'uuid': '...'} or contains it
                uuid_val = result['uuid']
                self.save_account(uuid_val)
                successful += 1
            
            if i < count - 1:
                delay = random.uniform(min_delay, max_delay)
                time.sleep(delay)
        
        print(f"\n[+] Finished! {successful}/{count} accounts created.")
        print(f"[+] Saved to: {self.output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hushra.me Account Generator")
    parser.add_argument_group("Generation Options")
    parser.add_argument("-c", "--count", type=int, default=1, help="Number of accounts to generate")
    parser.add_argument("-o", "--output", type=str, default="uuids.txt", help="Output TXT file")
    parser.add_argument("--min-delay", type=float, default=5.0, help="Min delay between requests")
    parser.add_argument("--max-delay", type=float, default=15.0, help="Max delay between requests")
    parser.add_argument("--proxies", type=str, help="Path to proxy list file (one per line)")

    args = parser.parse_args()

    proxies = None
    if args.proxies:
        try:
            with open(args.proxies, 'r') as f:
                proxies = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"[!] Proxy file not found: {args.proxies}")
            sys.exit(1)

    gen = HushraGenerator(output_file=args.output, proxy_list=proxies)
    gen.run(count=args.count, min_delay=args.min_delay, max_delay=args.max_delay)
