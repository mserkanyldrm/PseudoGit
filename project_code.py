import os
import socket
import ssl
import threading
import base64
import json
from urllib.parse import quote


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
        
    def clone_repository(self, repo_owner, repo_name, branch='main', parallel_count=1):
        endpoints = pseudo_git.get_repository_endpoints(repo_owner, repo_name)

        if not endpoints:
            print("Failed to get repository endpoints.")
            return

        file_datas = self.get_filedatas(endpoints)

        # Thread list for handling downloads concurrently
        thread_single_files = []

        # Using a lock for thread-safe printing or other shared resources if needed
        print_lock = threading.Lock()

        def download_worker(file_data):
            """Worker function for downloading files in parallel"""
            if file_data.size < self.MB_SIZE:
                file_content = pseudo_git.download_file(repo_owner, repo_name, branch, file_data.path)
            else:
                file_content = pseudo_git.download_large_file(repo_owner, repo_name, branch, file_data.path, file_data.size, parallel_count)

            if file_content:
                # Ensure the directory structure exists
                directory = os.path.dirname(file_data.path)
                try:
                    # Check and create directory if needed
                    if directory and not os.path.exists(directory):
                        os.makedirs(directory, exist_ok=True)  # Use exist_ok=True for concurrency safety

                    # Check if the path is indeed a file, not a directory
                    if os.path.isdir(file_data.path):
                        with print_lock:
                            print(f"Error: '{file_data.path}' is a directory, not a file.")
                        return

                    # Write the file content to the path
                    with open(file_data.path, "wb") as f:
                        f.write(file_content)
                    with print_lock:
                        print(f"File '{file_data.name}' downloaded successfully.")

                except PermissionError as e:
                    with print_lock:
                        print(f"PermissionError: Could not write to '{file_data.path}'. Check permissions. {e}")

                except FileNotFoundError as e:
                    with print_lock:
                        print(f"FileNotFoundError: Directory '{directory}' could not be created. {e}")

                except Exception as e:
                    with print_lock:
                        print(f"An unexpected error occurred: {e}")
            else:
                with print_lock:
                    print(f"Failed to download file '{file_data.name}'.")

        # Iterate over file datas and create threads
        for file_data in file_datas:
            if parallel_count > 1 and file_data.size < self.MB_SIZE:
                # Create a thread for smaller files if parallel download is enabled
                t = threading.Thread(target=download_worker, args=(file_data,))
                thread_single_files.append(t)

                # Start threads in batches of `parallel_count`
                if len(thread_single_files) == parallel_count:
                    print(f"Starting threads for single files | Count: {len(thread_single_files)}")
                    for t in thread_single_files:
                        t.start()
                    for t in thread_single_files:
                        t.join()
                    thread_single_files = []

            else:
                # Download large files or if parallel_count is 1 (single-threaded)
                download_worker(file_data)

        # Start any remaining threads
        if thread_single_files:
            print(f"Starting remaining threads for single files | Count: {len(thread_single_files)}")
            for t in thread_single_files:
                t.start()
            for t in thread_single_files:
                t.join()



    def get_filedatas(self, endpoints):
        filedatas = []

        # Directly iterate over the fetched endpoints
        for endpoint in endpoints:
            if endpoint["type"] == "file":
                file_data = GithubFileData(
                    endpoint["name"], endpoint["path"], endpoint["sha"], endpoint["size"], endpoint["download_url"]
                )
                filedatas.append(file_data)

        return filedatas



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
    repo_owner = "paperless-ngx"
    repo_name = "paperless-ngx"
    branch = "main"

    pseudo_git = GitHubPseudoGit(username, token)
    
    pseudo_git.clone_repository(repo_owner, repo_name, branch, parallel_count=4)

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
