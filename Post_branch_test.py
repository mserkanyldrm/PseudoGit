import socket
import ssl
import base64
import json

class GitHubBranchCreator:
    def __init__(self, username, token):
        self.username = username
        self.token = token
        self.auth_header = self._create_authorization_header()

    def _create_authorization_header(self):
        credentials = f"{self.username}:{self.token}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Authorization: Basic {encoded_credentials}"

    def _create_secure_socket(self, hostname, port=443):
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        context = ssl.create_default_context()
        return context.wrap_socket(tcp_socket, server_hostname=hostname)

    def _send_request(self, hostname, request):
        client_socket = self._create_secure_socket(hostname)
        client_socket.connect((hostname, 443))

        try:
            client_socket.sendall(request.encode())

            response = b""
            while True:
                data = client_socket.recv(4096)
                if not data:
                    break
                response += data

            headers, _, body = response.partition(b"\r\n\r\n")
            return headers.decode(), body.decode()
        finally:
            client_socket.close()

    def get_commit_sha(self, repo_owner, repo_name, branch_name):
        hostname = "api.github.com"
        request = (
            f"GET /repos/{repo_owner}/{repo_name}/git/ref/heads/{branch_name} HTTP/1.1\r\n"
            f"Host: {hostname}\r\n"
            f"{self.auth_header}\r\n"
            f"User-Agent: PseudoGit/1.0\r\n"
            f"Accept: application/vnd.github.v3+json\r\n"
            f"Connection: close\r\n\r\n"
        )

        headers, body = self._send_request(hostname, request)
        if "200 OK" in headers:
            response_data = json.loads(body)
            return response_data['object']['sha']
        else:
            print(f"Failed to get the SHA for branch {branch_name}. Headers: {headers}")
            return None

    def create_branch(self, repo_owner, repo_name, new_branch_name, source_branch_name='main'):
        sha = self.get_commit_sha(repo_owner, repo_name, source_branch_name)

        if not sha:
            print(f"Cannot create branch without a valid SHA from source branch: {source_branch_name}")
            return

        hostname = "api.github.com"
        data = {
            "ref": f"refs/heads/{new_branch_name}",
            "sha": sha
        }

        json_data = json.dumps(data)
        content_length = len(json_data)

        request = (
            f"POST /repos/{repo_owner}/{repo_name}/git/refs HTTP/1.1\r\n"
            f"Host: {hostname}\r\n"
            f"{self.auth_header}\r\n"
            f"User-Agent: PseudoGit/1.0\r\n"
            f"Accept: application/vnd.github.v3+json\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {content_length}\r\n"
            f"Connection: close\r\n\r\n"
            f"{json_data}"
        )

        headers, body = self._send_request(hostname, request)
        if "201 Created" in headers:
            print(f"Branch '{new_branch_name}' created successfully.")
        else:
            print(f"Failed to create branch '{new_branch_name}'. Headers: {headers}, Body: {body}")


# Example usage
if __name__ == "__main__":
    username = "mserkanyldrm"
    token = ""
    repo_owner = "mserkanyldrm"
    repo_name = "CS421-PA1_Fall2024"
    new_branch_name = "new-test-branch1"  # Change to the desired branch name
    source_branch_name = "main"  # Change if you want to use a different source branch

    branch_creator = GitHubBranchCreator(username, token)
    branch_creator.create_branch(repo_owner, repo_name, new_branch_name, source_branch_name)
