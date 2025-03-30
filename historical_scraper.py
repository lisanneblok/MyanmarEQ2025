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
# custom
import config, utils
import traceback


class BlueskyHistoricalSearch:
    def __init__(self, username, password, output_folder=None, output_file=None, verbose=False):
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
        
        # Stats tracking
        self.post_count = 0
        self.image_post_count = 0
        self.start_time = None
        self.output_folder = output_folder
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
    
    def search_posts(self, 
                    keywords,
                    start_time=None, 
                    num_days_prev=1,
                    end_time=None, 
                    limit=100, 
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
        # Search for each keyword or use a generic search if keywords is None
        keyword_list = keywords.split(',') if keywords else [""]
        
        for keyword in keyword_list:
            if self.verbose and keyword:
                print(f"Searching for keyword: {keyword}")
            elif self.verbose:
                print("Collecting recent posts (no keyword filter)")
            
            cursor = None
            while posts_collected < limit:
                try:
                    # Use the search_posts endpoint
                    params = {"limit": min(100, limit - posts_collected), "q": keyword or "*"}
                    if cursor:
                        params["cursor"] = cursor
                    
                    response = self.client.app.bsky.feed.search_posts(params)
                    
                    if not response or not hasattr(response, 'posts') or not response.posts:
                        if self.verbose:
                            print(f"No more results for '{keyword or 'general search'}'")
                        break
                        
                    # Process each post
                    for post in response.posts:
                        # Check if post is within time window
                        post_time = datetime.fromisoformat(post.indexed_at.replace('Z', '+00:00'))

                        if start_time <= post_time <= end_time:
                            # Extract post data
                            post_data = self._extract_post_data(post)
                            
                            # Apply filters
                            if only_images and not post_data['has_images']:
                                continue
                            
                            if not include_replies and post_data['reply_to']:
                                continue
                            
                            # Save the post
                            self._save_post_data(post_data)
                            all_results.append(post_data)
                            posts_collected += 1
                            
                            if posts_collected >= limit:
                                break
                        
                        # Get cursor for pagination
                        cursor = response.cursor if hasattr(response, 'cursor') else None
                        if not cursor:
                            if self.verbose:
                                print(f"No more pages for '{keyword or 'general search'}'")
                            break
                        
                        # Rate limiting
                        time.sleep(0.5)
                        
                except Exception as e:
                    print(f"Error during search: {e}")
                    print(f"Full error details: {type(e).__name__}: {str(e)}")
                    print(traceback.format_exc())
                    time.sleep(2)  # Wait longer on error
                    break
        
        # Print summary
        self._print_summary()
        
        return all_results
    
    def _extract_post_data(self, post):
        """
        Extract relevant data from a post
        
        Args:
            post: The post object from the API
            
        Returns:
            dict: Extracted post data
        """
        # Check for images
        has_images = False
        if hasattr(post, 'embed'):
            embed = post.embed
            if hasattr(embed, '$type'):
                has_images = (
                    embed.type == 'app.bsky.embed.images' or
                    (embed.type == 'app.bsky.embed.external' and hasattr(embed, 'thumb'))
                )
        
        # Check for reply
        reply_to = None
        if hasattr(post, 'reply') and post.reply:
            if hasattr(post.reply, 'parent') and hasattr(post.reply.parent, 'uri'):
                reply_to = post.reply.parent.uri
        
        # Get post text
        text = post.record.text if hasattr(post, 'record') and hasattr(post.record, 'text') else ''
        
        # Convert AT URI to web URL
        web_url = None
        if hasattr(post, 'uri') and hasattr(post.author, 'handle'):
            try:
                post_id = post.uri.split('/')[-1]
                web_url = f"https://bsky.app/profile/{post.author.handle}/post/{post_id}"
            except:
                pass
        
        return {
            'text': text,
            'created_at': post.indexed_at if hasattr(post, 'indexed_at') else '',
            'author': post.author.handle if hasattr(post, 'author') and hasattr(post.author, 'handle') else '',
            'uri': post.uri if hasattr(post, 'uri') else '',
            'has_images': has_images,
            'reply_to': reply_to,
            'web_url': web_url,
            'likes': post.likeCount if hasattr(post, 'likeCount') else 0,
            'reposts': post.repostCount if hasattr(post, 'repostCount') else 0
        }
    
    def _save_post_data(self, post_data):
        """
        Save post data to the output file
        
        Args:
            post_data (dict): Post data to save
        """
        print(self.output_folder)
        self.output_file = self.output_folder / f"bluesky_posts.jsonl"
    
        print(f"Saving post data to {self.output_file}")
        # Create the output file if it doesn't exist
        if not Path(self.output_file).exists():
            with open(self.output_file, 'w') as f:
                f.write('')
            print(f"Created output file: {self.output_file}")
                
        with open(self.output_file, 'a') as f:
            json.dump(post_data, f)
            f.write('\n')
        
        self.post_count += 1
        if post_data['has_images']:
            self.image_post_count += 1
            
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
        print(f"Average rate: {rate:.1f} posts/sec")
        if self.post_count > 0:
            print(f"Output saved to: {self.output_file}")
        else:
            print("No posts collected.")
    
    def process_results(self, output_folder=None):
        """
        Process the collected results and generate analysis
        
        Args:
            output_folder (str, optional): Folder to save results. If None, uses current directory.
        """
        
        # os.makedirs(output_folder, exist_ok=True)
        
        # Load the collected data
        posts = []
        with open(self.output_file, 'r') as f:
            for line in f:
                posts.append(json.loads(line))
        
        if not posts:
            print("No posts to analyze")
            return
        
        # Create a DataFrame
        df = pd.DataFrame(posts)
        
        # Basic analysis
        analysis = {
            'total_posts': len(df),
            'posts_with_images': df['has_images'].sum(),
            'unique_authors': df['author'].nunique(),
            'top_authors': df['author'].value_counts().head(10).to_dict(),
            'avg_likes': df['likes'].mean() if 'likes' in df.columns else 0,
            'avg_reposts': df['reposts'].mean() if 'reposts' in df.columns else 0,
        }
        
        # Convert NumPy types to native Python types for JSON serialization
        def convert_to_python_types(obj):
            if isinstance(obj, dict):
                return {k: convert_to_python_types(v) for k, v in obj.items()}
            elif hasattr(obj, 'tolist'):  # Handle NumPy arrays and scalars
                return obj.tolist()
            elif hasattr(obj, 'item'):    # Handle NumPy scalar types
                return obj.item()
            else:
                return obj
                
        # Save analysis
        with open(f"{self.output_folder}/analysis.json", 'w') as f:
            json.dump(convert_to_python_types(analysis), f, indent=2)
        
        # Save DataFrame
        df.to_csv(f"{self.output_folder}/processed_posts.csv", index=False)
        
        print(f"Analysis complete! Results saved to {self.output_folder}")
        return analysis

# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bluesky Historical Search")
    parser.add_argument("-usrnm", "--username", type=str, help="Bluesky username or email", default=None)
    parser.add_argument("-psswrd", "--password", type=str, help="Bluesky password", default=None)
    parser.add_argument("-days", "--num_days_prev", type=int, help="Number of days to search back", default=1)
    parser.add_argument("-kwargs", "--keywords", type=str, help="Comma-separated keywords to search for")
    parser.add_argument("-ims", "--only_images", action='store_true', help="Only include posts with images")
    parser.add_argument("-lim", "--limit", type=int, help="Limit the number of posts to retrieve", default=10)
    
    args = parser.parse_args()
    # set up files
    output_folder = Path(f"bluesky_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    output_folder.mkdir(parents=True, exist_ok=True)
    
    auth_file = utils.read_yaml(config.get_repo_root() / "auth.yaml")
    searcher = BlueskyHistoricalSearch(
        username=auth_file["bluesky_auth"]['username'] if args.username is None else args.username,
        password=auth_file["bluesky_auth"]['password'] if args.password is None else args.password,
        output_folder=output_folder,
        verbose=True
    )
    
    # Search for posts in the last 7 days
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
    searcher.process_results(output_folder=output_folder)