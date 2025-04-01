import subprocess
from pathlib import Path

# get module working directory
def get_repo_root():
    # Run 'git rev-parse --show-toplevel' command to get the root directory of the Git repository
    git_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    )
    if git_root.returncode == 0:
        return Path(git_root.stdout.strip())
    else:
        raise RuntimeError("Unable to determine Git repository root directory.")

repo_dir = get_repo_root()

auth_fp = repo_dir / "auth.yaml"

bluesky_data_dir = repo_dir / "bluesky_data"
bluesky_historic_searches_dir = bluesky_data_dir / "historic_searches"
bluesky_images_dir = bluesky_data_dir / "images"
# bluesky_image_urls_fp = bluesky_data_dir / "image_urls.csv"
bluesky_processed_posts_fp = bluesky_data_dir / "processed_posts.csv"