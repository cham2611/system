from flask import Flask, render_template, Response
import cv2
import subprocess
import threading
import time
import os
from flask_cors import CORS

# Flask 인스턴스 생성
app = Flask(__name__)
CORS(app)

# 객체 탐지 시작 함수
def start_object_detection():
    # 객체 탐지 파이썬 스크립트 실행
    command = 'python Exdetect1.py --weights yolov5s.pt --img 640 --conf 0.4 --source 0 --classes 0'
    subprocess.Popen(command, shell=True)

# 프레임 생성 함수
detection_started = False

# gen_frames 함수를 추가합니다.
def gen_frames():
    global detection_started
    # 객체 탐지가 시작되지 않았다면
    if not detection_started:
        # 새로운 스레드에서 객체 탐지 시작
        t = threading.Thread(target=start_object_detection)
        t.daemon = True
        t.start()
        detection_started = True
    # 무한 루프
    while True:
        # 객체 탐지 결과 이미지 파일이 존재한다면
        if os.path.exists("path_to_detection_output.jpg"):
            # 이미지 파일 읽기
            frame = cv2.imread("path_to_detection_output.jpg")
            # 이미지를 JPEG로 인코딩
            ret, buffer = cv2.imencode('.jpg', frame)
            frame = buffer.tobytes()
            # 프레임 생성
            yield (b'--frame\r\nContent-Type: image/jpeg\r\n\r\n' + frame + b'\r\n')
        # 1초 대기
        time.sleep(0.2555)  # 이미지 파일이 존재하는지 확인한 후에 딜레이를 줍니다.



# 비디오 피드 라우트
@app.route('/video_feed')
def video_feed():
    # 응답으로 프레임 생성
    return Response(gen_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')

# 인덱스 라우트
@app.route('/')
def index():
    # 인덱스 라우트
    return render_template('index.html')
# 메인 함수
if __name__ == '__main__':
    # Flask 앱 실행
    app.run(host='0.0.0.0', port=5000,debug=True)