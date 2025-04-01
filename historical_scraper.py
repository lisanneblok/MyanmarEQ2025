import requests
import json
import time
from datetime import datetime, timedelta

# import os
from pathlib import Path
from atproto import Client
import pandas as pd
from tqdm import tqdm
import argparse
import traceback
from deep_translator import GoogleTranslator

# custom
import config, utils


class BlueskyHistoricalSearch:
    def __init__(self, username, password, output_file=None, verbose=False):
        """
        Initialize the historical search client
        
        Args:
            username (str): Bluesky username or email
            password (str): Bluesky password
            output_file (str, optional): Output file path. If None, one will be generated.
            verbose (bool, optional): Whether to print verbose output.
        """
        self.username = username
        self.password = password
        self.client = Client()
        self.authenticated = False
        self.verbose = verbose
        self.request_timeout = 5  # Timeout for requests in seconds
        
        # Stats tracking
        self.post_count = 0
        self.image_post_count = 0
        self.start_time = None
        self.output_file = output_file
    
    def authenticate(self):
        """Authenticate with Bluesky"""
        try:
            self.client.login(self.username, self.password)
            self.authenticated = True
            if self.verbose:
                print(f"Authentication successful for {self.username}")
            return True
        except Exception as e:
            print(f"Authentication failed: {e}")
            return False
    
    
    def _make_request_with_backoff(self, func, params, max_retries=3):
        retries = 0
        backoff_time = 0.5
        
        while retries < max_retries:
            try:
                return func(params, timeout=self.request_timeout)
            except requests.exceptions.Timeout:
                print(f"Request timed out after {self.request_timeout} seconds.")
            except Exception as e:
                print(f"Request error: {e}")
            
            retries += 1
            sleep_time = backoff_time * (2 ** (retries - 1))  # Exponential backoff
            print(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
        
        return None


    def search_posts(self, 
                    keywords,
                    start_time=None, 
                    num_days_prev=1,
                    end_time=None, 
                    limit=None, 
                    only_images=False, 
                    include_replies=False):
        """
        Search for historical posts matching criteria
        
        Args:
            keywords (list): List of keywords to search for
            start_time (datetime, optional): Start time for search window
            end_time (datetime, optional): End time for search window
            limit (int, optional): Maximum number of posts to retrieve
            only_images (bool, optional): Only retrieve posts with images
            include_replies (bool, optional): Include reply posts in results
            
        Returns:
            list: List of matching posts
        """
        if not self.authenticated and not self.authenticate():
            print("Not authenticated. Cannot perform search.")
            return []
        
        self.start_time = utils.specify_utc(datetime.fromtimestamp(time.time()))
        
        # Format time parameters
        if start_time is None:
            start_time = datetime.now() - timedelta(days=num_days_prev)
        if end_time is None:
            end_time = utils.specify_utc(datetime.fromtimestamp(datetime.now()))
            
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        start_time = utils.specify_utc(start_time)
        end_time = utils.specify_utc(end_time)
        
        if self.verbose:
            print(f"Searching for posts from {start_str} to {end_str}")
            print(f"Keywords: {keywords}")
        
        all_results = []
        posts_collected = 0
        images_collected = 0
        # Search for each keyword or use a generic search if keywords is None
        keyword_list = [k.strip() for k in keywords.split(',')] if keywords else [""]
        
        for keyword in keyword_list:
            if self.verbose and keyword:
                print(f"Searching for keyword: {keyword}")
            elif self.verbose:
                print("Collecting recent posts (no keyword filter)")
            
            cursor = None
            while limit is None or posts_collected < limit:
                try:
                    # Use the search_posts endpoint with timeout
                    params = {"limit": 100 if limit is None else min(100, limit - posts_collected), "q": keyword or "*"}
                    if cursor:
                        params["cursor"] = cursor
                    
                    # Set a timeout for the request
                    response = None
                    try:
                        response = self._make_request_with_backoff(
                            self.client.app.bsky.feed.search_posts, params
                        )
                        # response = self.client.app.bsky.feed.search_posts(params, timeout=self.request_timeout)
                    except requests.exceptions.Timeout:
                        print(f"Request timed out after {self.request_timeout} seconds. Moving to next request.")
                        time.sleep(1)
                        continue
                    except Exception as e:
                        print(f"Request error: {e}")
                        time.sleep(1)
                        continue
                    
                    if not response or not hasattr(response, 'posts') or not response.posts:
                        if self.verbose:
                            print(f"No more results for '{keyword or 'general search'}'")
                        break
                        
                    for post in response.posts:
                        # check if post is within time window
                        post_time = datetime.fromisoformat(post.indexed_at.replace('Z', '+00:00'))
                        if start_time <= post_time <= end_time:
                            # extract post data
                            post_data = self._extract_post_data(post)
                            # skip if post is empty (filtered out in extract)
                            if not post_data:
                                continue
                            
                            # apply filters
                            if only_images and not post_data['has_images']:
                                continue
                            else:
                                images_collected += len(post_data['image_urls']) if post_data['has_images'] else 0
                            
                            if not include_replies and post_data['reply_to']:
                                continue
                            
                            outcome = self._save_post_data(post_data)
                            
                            all_results.append(post_data)
                            if outcome:
                                posts_collected += 1
                            
                            if posts_collected >= limit if limit else 0:
                                break
                        
                        # Get cursor for pagination
                        cursor = response.cursor if hasattr(response, 'cursor') else None
                        # if cursor and (limit is None or posts_collected < limit):
                        #     continue  # Move to next page even if current page had errors
                        # else:
                        #     break  # Only break if we have no cursor or hit our limit
                        
                        if images_collected > 0 and images_collected % 10 == 0:
                            print("\nSTASHING IMAGES...")
                            all_urls = set([url for post in all_results if post['has_images'] for url in post['image_urls']])
                            # load file of existing urls
                            new_urls = self._download_new_images(all_urls)
                            self.num_new_images = len(new_urls)
                            all_urls = []
                        
                except Exception as e:
                    print(f"Error during search: {e}")
                    print(f"Full error details: {type(e).__name__}: {str(e)}")
                    print(traceback.format_exc())
                    time.sleep(2)  # Wait longer on error
                    break
                
        all_urls = set([url for post in all_results if post['has_images'] for url in post['image_urls']])
        # download non-duplicate images
        new_urls = self._download_new_images(all_urls)
        self.num_new_images = len(new_urls)
                
        self._print_summary()
        
        return all_results
    
    def _download_new_images(self, image_urls):
        if not config.bluesky_processed_posts_fp.exists():
            return image_urls
        else:
            # read existing urls from csv
            existing_urls = pd.read_csv(config.bluesky_processed_posts_fp)['image_urls'].tolist()
            existing_urls = [url for sublist in existing_urls for url in sublist.split(',')]  # flatten list
        
        # compare differences
        new_urls = set(image_urls) - set(existing_urls)
        # download new urls
        [self._download_image(image_url) for image_url in new_urls]
        return new_urls
            
    def _extract_post_data(self, post):
        """
        Extract relevant data from a post
        
        Args:
            post: The post object from the API
            
        Returns:
            dict: Extracted post data
        """
        has_images = False
        image_urls = []
        # check for images
        if hasattr(post, 'embed'):
            embed = post.embed
            if embed:
                if 'images' in embed.__dict__:
                    has_images = True
                    # append urls to list (for later download)
                    image_urls = self._get_image_data(post)

        # check for reply
        reply_to = None
        if hasattr(post, 'reply') and post.reply:
            if hasattr(post.reply, 'parent') and hasattr(post.reply.parent, 'uri'):
                reply_to = post.reply.parent.uri
        
        # get post text
        text = post.record.text if hasattr(post, 'record') and hasattr(post.record, 'text') else ''
        # translate text to English if needed
        original_text = text
        if text:
            translated_text = GoogleTranslator(source='auto', target='en').translate(text)
            # only mark as translated if the text actually changed
            if translated_text != original_text:
                text = f"{translated_text}"
            else:
                text = translated_text
        else:
            text = ''        
        
        # convert AT URI to web URL
        web_url = None
        if hasattr(post, 'uri') and hasattr(post.author, 'handle'):
            try:
                post_id = post.uri.split('/')[-1]
                web_url = f"https://bsky.app/profile/{post.author.handle}/post/{post_id}"
            except:
                pass
        
        author = post.author.handle if hasattr(post, 'author') and hasattr(post.author, 'handle') else ''
        if author == "nowbreezing.ntw.app": # excluding a spammy wordcloud account
            return {}
        
        return {
            'cid': post.cid if hasattr(post, 'cid') else '',
            'text': original_text,
            'translated_text': text,
            'created_at': post.indexed_at if hasattr(post, 'indexed_at') else '',
            'author': post.author.handle if hasattr(post, 'author') and hasattr(post.author, 'handle') else '',
            'translated': 'yes' if text != original_text else 'no',
            'uri': post.uri if hasattr(post, 'uri') else '',
            'has_images': has_images,
            'image_urls': image_urls if has_images else [],
            'reply_to': reply_to,
            'web_url': web_url,
            'likes': post.likeCount if hasattr(post, 'likeCount') else 0,
            'reposts': post.repostCount if hasattr(post, 'repostCount') else 0
        }
        
    def _get_image_data(self, post):
        image_urls = []
        if hasattr(post, 'embed') and post.embed:
            image_list = post.embed.__dict__.get('images')[0]   # TODO: check if multiple images               
            image_urls.append(image_list['fullsize'])

        return image_urls
            
    def _download_image(self, image_url):
        """
        Download an image from a URL
        
        Args:
            image_url (str): URL of the image to download
        """
        try:
            # add timeout to the request
            response = requests.get(image_url, timeout=self.request_timeout)
            if response.status_code == 200:
                image_fp = config.bluesky_images_dir / f"{image_url.split('/')[-1]}"
                # replace @jpeg with .jpeg
                image_fp = image_fp.with_name(image_fp.name.replace('@jpeg', '.jpeg'))
                # # add url to csv file
                # utils.append_to_csv(config.bluesky_image_urls_fp, [image_url], mode='a')
                
                with open(image_fp, 'wb') as f:
                    f.write(response.content)
            else:
                print(f"Failed to download image: {image_url}")
                return None
        except requests.exceptions.Timeout:
            print(f"Image download timed out after {self.request_timeout} seconds: {image_url}")
            return None
        except Exception as e:
            print(f"Error downloading image: {e}")
            return None
            
    def _save_post_data(self, post_data):
        """
        Save post data to the output file
        
        Args:
            post_data (dict): Post data to save
        """
        # uri_file = config.bluesky_post_uris_fp
        existing_posts_fp = config.bluesky_processed_posts_fp
        self.output_file = existing_posts_fp
        
        if existing_posts_fp.exists():
            # read existing urls from csv
            existing_uris = pd.read_csv(existing_posts_fp)['uri'].tolist()
            # print(existing_uris)
        else:
            existing_posts_fp.touch(exist_ok=True)
            # create empty file
            with open(existing_posts_fp, 'w') as f:
                # write the header as the post_data keys
                f.write(','.join(post_data.keys()) + '\n')
            print(f"Created output file: {self.output_file}")
            existing_uris = []
                
        # print(post_data['uri'])
        # check if post is already in file
        if post_data['uri'] in existing_uris:
            print(f"Post already exists in file: {post_data['uri']}")
            return False
        else:
            # add post data to csv file
            utils.append_to_csv(self.output_file, [post_data], mode='a')
            self.post_count += 1
            if post_data['has_images']:
                self.image_post_count += 1
            return True
            
        if self.verbose:
            print(f"Saved post by @{post_data['author']}: {post_data['text'][:50]}...")
            
    
    def _print_summary(self):
        """Print a summary of the search results"""
        elapsed = utils.specify_utc(datetime.fromtimestamp(time.time())) - self.start_time if self.start_time else 0
        elapsed = (utils.specify_utc(datetime.fromtimestamp(time.time())) - self.start_time).total_seconds() if self.start_time else 0
        rate = self.post_count / elapsed if elapsed > 0 else 0
        
        print("\nSearch complete!")
        print(f"Collected {self.post_count} posts in {elapsed:.2f} seconds")
        print(f"Posts with images: {self.image_post_count} ({self.image_post_count/self.post_count*100:.1f}% of total)") if self.post_count > 0 else print("No posts collected.")
        print(f"Number of new images: {self.num_new_images}")
        print(f"Average rate: {rate:.1f} posts/sec")
        if self.post_count > 0:
            print(f"Output saved to: {self.output_file}")
        else:
            print("No posts collected.")
    
    def process_results(self):
        """
        Process the collected results and generate analysis
        """
                
        # load the collected data
        try:
            df = pd.read_csv(self.output_file)
        except TypeError:
            print("No posts to analyze (output file not found)")
            return
        
        if df.empty:
            print("No posts to analyze")
            return

        # store basic analysis
        analysis = {
            'total_posts': len(df),
            'posts_with_images': df['has_images'].sum(),
            'unique_authors': df['author'].nunique(),
            'top_authors': df['author'].value_counts().head(10).to_dict(),
            'avg_likes': df['likes'].mean() if 'likes' in df.columns else 0,
            'avg_reposts': df['reposts'].mean() if 'reposts' in df.columns else 0,
        }
        
        # convert NumPy types to native Python types for JSON serialization
        def convert_to_python_types(obj):
            if isinstance(obj, dict):
                return {k: convert_to_python_types(v) for k, v in obj.items()}
            elif hasattr(obj, 'tolist'):  # Handle NumPy arrays and scalars
                return obj.tolist()
            elif hasattr(obj, 'item'):    # Handle NumPy scalar types
                return obj.item()
            else:
                return obj
    
        # save analysis
        analysis_fp = f"{config.bluesky_historic_searches_dir}/analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json"
        with open(analysis_fp, 'w') as f:
            json.dump(convert_to_python_types(analysis), f, indent=2)
        
        df.to_csv(config.bluesky_processed_posts_fp, index=False)
        
        print(f"Analysis complete! Results saved to {analysis_fp}")
        return analysis


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bluesky Historical Search")
    parser.add_argument("-usrnm", "--username", type=str, help="Bluesky username or email", default=None)
    parser.add_argument("-psswrd", "--password", type=str, help="Bluesky password", default=None)
    parser.add_argument("-days", "--num_days_prev", type=int, help="Number of days to search back", default=1)
    parser.add_argument("-kwargs", "--keywords", type=str, help="Comma-separated keywords to search for")
    parser.add_argument("-ims", "--only_images", action='store_true', help="Only include posts with images")    # TODO: make this do its legwork
    parser.add_argument("-lim", "--limit", type=int, help="Limit the number of posts to retrieve", default=None)
    args = parser.parse_args()
    
    # set up files for saving results
    config.bluesky_images_dir.mkdir(parents=True, exist_ok=True)
    config.bluesky_historic_searches_dir.mkdir(parents=True, exist_ok=True)
    
    auth_file = utils.read_yaml(config.get_repo_root() / "auth.yaml")
    searcher = BlueskyHistoricalSearch(
        username=auth_file["bluesky_auth"]['username'] if args.username is None else args.username,
        password=auth_file["bluesky_auth"]['password'] if args.password is None else args.password,
        verbose=True
    )
    
    # search for posts in the last n days
    start_time = datetime.now() - timedelta(days=args.num_days_prev)
    end_time = datetime.now()
    
    results = searcher.search_posts(
        keywords=args.keywords,
        start_time=start_time,
        end_time=end_time,
        limit=args.limit,
        only_images=args.only_images,
    )
    
    # Process and analyze results
    searcher.process_results()