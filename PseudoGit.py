import hashlib
import os
import socket
import ssl
import threading
import base64
import json
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed


class GitHubHttpClient:
    def __init__(self, hostname, port=443):
        self.hostname = hostname
        self.port = port
        self.socket = None

    def _create_secure_socket(self):
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        context = ssl.create_default_context()
        return context.wrap_socket(tcp_socket, server_hostname=self.hostname)

    def connect(self):
        try:
            self.socket = self._create_secure_socket()
            self.socket.connect((self.hostname, self.port))
        except ssl.SSLError as e:
            print(f"SSL error occurred: {e}")
            self.close()
        except socket.error as e:
            print(f"Socket error occurred: {e}")
            self.close()

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def send_request(self, request):
        try:
            self.socket.sendall(request.encode())
            return self._receive_response()
        except Exception as e:
            print(f"Failed to send HTTP request: {e}")
            self.close()
            return None

    def _receive_response(self):
        response = b""
        try:
            while True:
                data = self.socket.recv(4096)
                if not data:
                    break
                response += data
            return response
        except Exception as e:
            print(f"Error receiving response: {e}")
        finally:
            self.close()

class GitHubPseudoGit:

    MB_SIZE = 1024 * 1024
    def __init__(self, username, token):
        self.username = username
        self.token = token
        self.auth_header = self._create_authorization_header()

    def _create_authorization_header(self):
        credentials = f"{self.username}:{self.token}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Authorization: Basic {encoded_credentials}"

    def get_repository_tree(self, repo_owner, repo_name, branch='main'):
        # Use the Git Tree API to get the entire repository tree in one request
        request = (
            f"GET /repos/{repo_owner}/{repo_name}/git/trees/{branch}?recursive=1 HTTP/1.1\r\n"
            f"Host: api.github.com\r\n"
            f"{self.auth_header}\r\n"
            f"User-Agent: PseudoGit/1.0\r\n"
            f"Connection: close\r\n\r\n"
        )
        
        # Create a client and send the request
        client = GitHubHttpClient("api.github.com")
        client.connect()
        response = client.send_request(request)
        
        if response:
            headers, _, body = response.partition(b"\r\n\r\n")
            
            try:
                # Parse the response body as JSON
                tree_data = json.loads(body)
                
                if "tree" in tree_data:
                    return tree_data  # Return the entire response to be processed later
                else:
                    print("Unexpected response format, missing 'tree' key.")
                    return None

            except json.JSONDecodeError as e:
                print(f"Failed to parse response as JSON: {e}")
                print(body)  # Optional: print the body to see what went wrong
                return None
        else:
            print("Failed to get repository tree.")
            return None

    
    def get_repository_endpoints(self, repo_owner, repo_name, file_path=None):

        response = pseudo_git.get_repository_tree(repo_owner, repo_name, branch)
        # print(json.dumps(response, indent=2))
        if not response or "tree" not in response:
            print("Failed to get repository tree.")
            return []

        endpoints = []
        for item in response["tree"]:
            # Each item represents a file or directory in the repository
            endpoint = {
                "name": item["path"].split("/")[-1],
                "path": item["path"],
                "sha": item.get("sha"),
                "size": item.get("size", 0),
                "type": "file" if item["type"] == "blob" else "dir",
                "download_url": None  # To be constructed later when needed
            }
            endpoints.append(endpoint)
        print("---------------------------------------------------------------------------------------")
        print("Endpoints: ")
        print(json.dumps(endpoints, indent=2))
        print("---------------------------------------------------------------------------------------")

        return endpoints
        
    def clone_repository(self, repo_owner, repo_name, branch='main', parallel_count=4):
        # Get repository endpoints
        endpoints = self.get_repository_endpoints(repo_owner, repo_name, branch)
        if not endpoints:
            print("Failed to get repository endpoints.")
            return

        # Split files into small and large files based on size
        small_files, large_files = self.get_filedatas(endpoints)

        print("Checking for missing files...")

        small_files = [file for file in small_files if not self.is_latest_version(file)]
        large_files = [file for file in large_files if not self.is_latest_version(file)] 

        # Print missing files
        missing_files = small_files + large_files
        
        if  len(missing_files) == 0:
            print("All files are up to date.")
            return  

        print("Total missing files: ", len(missing_files))
        print("Missing/Changed files: [",end=" ")
        for file_data in missing_files:
            print(file_data.name,end=" ,")
        print("]")
        # Download small files in parallel
        if small_files:
            with ThreadPoolExecutor(max_workers=parallel_count) as executor:
                future_to_file = {
                    executor.submit(self.download_file, repo_owner, repo_name, branch, file_data.path): file_data
                    for file_data in small_files
                }

                for future in as_completed(future_to_file):
                    file_data = future_to_file[future]
                    try:
                        file_content = future.result()
                        if file_content:
                            self.write_file(file_data, file_content)
                    except Exception as exc:
                        print(f"File '{file_data.name}' generated an exception: {exc}")

        # Download large files sequentially
        for large_file in large_files:
            print(f"Downloading large file: {large_file.name}")
            file_content = self.download_large_file(repo_owner, repo_name, branch, large_file.path, large_file.size, parallel_count)
            if file_content:
                self.write_file(large_file, file_content)
            else:
                print(f"Failed to download large file '{large_file.name}'.")

    def download_worker(self, file_data, repo_owner, repo_name, branch, parallel_count=1):
        """Handles downloading a file, either small or large."""
        if file_data.size < self.MB_SIZE:
            file_content = self.download_file(repo_owner, repo_name, branch, file_data.path)
        else:
            file_content = self.download_large_file(repo_owner, repo_name, branch, file_data.path, file_data.size, parallel_count=parallel_count)

        if file_content:
            self.write_file(file_data, file_content)
        else:
            print(f"Failed to download file '{file_data.name}'.")

    def write_file(self, file_data, file_content):
        """Ensures the directory structure exists and writes the file content."""
        directory = os.path.dirname(file_data.path)
        try:
            # Check and create directory if needed
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)  # Use exist_ok=True for concurrency safety

            # Check if the path is indeed a file, not a directory
            if os.path.isdir(file_data.path):
                print(f"Error: '{file_data.path}' is a directory, not a file.")
                return

            # Write the file content to the path
            with open(file_data.path, "wb") as f:
                f.write(file_content)
            print(f"File '{file_data.name}' downloaded successfully.")

        except PermissionError as e:
            print(f"PermissionError: Could not write to '{file_data.path}'. Check permissions. {e}")

        except FileNotFoundError as e:
            print(f"FileNotFoundError: Directory '{directory}' could not be created. {e}")

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

   

    def get_filedatas(self, endpoints):
        small_files = []
        large_files = []

        for endpoint in endpoints:
            if endpoint['type'] == 'file':
                file_data = GithubFileData(
                    endpoint["name"], endpoint["path"], endpoint["sha"], endpoint["size"], endpoint["download_url"]
                )
                if file_data.size < self.MB_SIZE:
                    small_files.append(file_data)
                else:
                    large_files.append(file_data)

        return small_files, large_files

    def is_latest_version(self, file_data):
        """
        Checks if the local version of the file matches the GitHub version using SHA1.
        """
        # Local path where the file should exist
        local_path = file_data.path

        # Check if the file exists locally
        if not os.path.exists(local_path):
            return False

        # Read the local file and calculate its hash
        try:
            with open(local_path, 'rb') as f:
                file_content = f.read()

            # Calculate SHA-1 as per GitHub's method: sha1("blob " + filesize + "\0" + data)
            blob_header = f"blob {len(file_content)}\0".encode()
            sha1_hash = hashlib.sha1(blob_header + file_content).hexdigest()

            # Compare the calculated hash to the GitHub file data SHA
            if sha1_hash == file_data.sha:
                return True
            else:
                return False

        except Exception as e:
            print(f"Error while calculating SHA for '{file_data.path}': {e}")
            return False

    def calculate_file_sha(self, file_path):
        """Calculates the SHA-1 hash of the given file."""
        sha1 = hashlib.sha1()
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    sha1.update(chunk)
            return sha1.hexdigest()
        except Exception as e:
            print(f"Failed to compute SHA-1 for {file_path}: {e}")
            return None

    def download_file(self, repo_owner, repo_name,branch, file_path):
        client = GitHubHttpClient("raw.githubusercontent.com")
        encoded_file_path = quote(file_path)
        request = (
            f"GET /{repo_owner}/{repo_name}/{branch}/{encoded_file_path} HTTP/1.1\r\n"
            f"Host: {client.hostname}\r\n"
            f"{self.auth_header}\r\n"
            f"User-Agent: PseudoGit/1.0\r\n"
            f"Accept: application/vnd.github.v3+json\r\n"
            f"Connection: close\r\n\r\n"
        )
        
        client.connect()

        response = client.send_request(request)
        if response:
            headers, _, body = response.partition(b"\r\n\r\n")

            return body
        else:
            print("Failed to download file blob.")
            return None

    def download_large_file(self, repo_owner, repo_name,branch, file_path, file_size, parallel_count=4):
        chunk_size = file_size // parallel_count
        chunks = [None] * parallel_count
        threads = []

        for i in range(parallel_count):
            start = i * chunk_size
            end = file_size - 1 if i == parallel_count - 1 else (start + chunk_size - 1)
            t = threading.Thread(
                target=self._download_chunk,
                args=(repo_owner, repo_name,branch, file_path, start, end, chunks, i)
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        return b"".join(chunks) if all(chunks) else None

    def _download_chunk(self, repo_owner, repo_name,branch, file_path, start, end, chunks, index):
        client = GitHubHttpClient("raw.githubusercontent.com")
        encoded_file_path = quote(file_path)
        request = (
            f"GET /{repo_owner}/{repo_name}/{branch}/{encoded_file_path} HTTP/1.1\r\n"
            f"Host: {client.hostname}\r\n"
            f"{self.auth_header}\r\n"
            f"User-Agent: PseudoGit/1.0\r\n"
            f"Range: bytes={start}-{end}\r\n"
            f"Connection: close\r\n\r\n"
        )
        
        print(f"Downloading chunk {index} from {start} to {end}...")
        
        client.connect()

        response = client.send_request(request)
        if response:
            headers, _, body = response.partition(b"\r\n\r\n")

            print(f"Downloaded chunk {index}.")

            chunks[index] = body
        else:
            print(f"Failed to download chunk {index}.")


class GithubFileData:
    def __init__(self, name,path ,sha, size, download_url=None):
        self.name = name
        self.sha = sha
        self.path = path
        self.size = size
        self.download_url = download_url

    def __str__(self):
        return f"Path: {self.name}, SHA: {self.sha}, Size: {self.size}, Download URL: {self.download_url}"


if __name__ == "__main__":
    username = "mserkanyldrm"
    token = ""
    repo_owner = "bluesky-social"
    repo_name = "social-app"
    branch = "main"

    pseudo_git = GitHubPseudoGit(username, token)
    
    pseudo_git.clone_repository(repo_owner, repo_name, branch, parallel_count=64)

    # response = pseudo_git.get_repository_tree(repo_owner, repo_name, branch)
    # print(json.dumps(response, indent=2))
    # if not response or "tree" not in response:
    #     print("Failed to get repository tree.")
    #     exit()

    # endpoints = []
    # for item in response["tree"]:
    #     # Each item represents a file or directory in the repository
    #     endpoint = {
    #         "name": item["path"].split("/")[-1],
    #         "path": item["path"],
    #         "sha": item.get("sha"),
    #         "size": item.get("size", 0),
    #         "type": "file" if item["type"] == "blob" else "dir",
    #         "download_url": None  # To be constructed later when needed
    #     }
    #     endpoints.append(endpoint)
    
    # print(json.dumps(endpoints, indent=2))

    # endpoints = pseudo_git.get_repository_endpoints(repo_owner, repo_name)
    # file_datas = pseudo_git.get_filedatas(endpoints)
    # for file_data in file_datas:
    #     print(file_data)

    # print(json.dumps(endpoints, indent=2))
