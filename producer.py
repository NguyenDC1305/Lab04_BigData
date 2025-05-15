import socket
# crawl comment from youtube
import time
import json
from youtube_comment_downloader import YoutubeCommentDownloader

downloader = YoutubeCommentDownloader()

def fetch_comments(video_id, max_cmt = 200):
    comments = downloader.get_comments_from_url(f"https://www.youtube.com/watch?v={video_id}")
    batch = []
    cmt = 0
    for comment in comments:
        if cmt < max_cmt:
            temp = comment["text"]
            data = {"comment": temp}
            batch.append(data)
            cmt = cmt+1
        else:
            break
    return batch
video_id = "7u98x3_r4_A"
data = fetch_comments(video_id)

print(data[-1])

HOST = 'localhost'
PORT = 2301

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen()
print(f"Server listening on {HOST}:{PORT}")

conn, addr = server.accept()
print(f"Connected by {addr}")

batch_size = 20
i = 0
for i in range(0, len(data), batch_size):
    item = data[i:i+batch_size]
    line = json.dumps(item, ensure_ascii=False) + '\n'
    conn.sendall(line.encode())
    print(f"đã gửi {i/batch_size + 1} batch")
    time.sleep(10)

done_msg = {"comment":"DONE"}
terminate_line = json.dumps(done_msg, ensure_ascii=False) + '\n'
conn.sendall(terminate_line.encode())
print("sended terminate signal")
if conn.recv(1024).decode() == "OK200":
    conn.close()
    print("connection was closed")