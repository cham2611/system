<%@ page import="java.sql.*" %>
<%@ page import="java.util.*" %>
<%@ page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.PreparedStatement"%>
<%@page import="java.sql.DriverManager"%>
<%@page import="java.sql.Connection"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%
// DB 연결 및 SQL 쿼리 실행
Class.forName("com.mysql.jdbc.Driver");
String dbUrl = "jdbc:mysql://localhost:3306/jspbookdb?allowMultiQueries=true";
String dbUser = "root";
String dbPass = "1234";
Connection con = DriverManager.getConnection
		(dbUrl, dbUser, dbPass);

String sql1 = "select * from sum_count1 ORDER BY sum_num DESC LIMIT 1; ";
PreparedStatement pstmt1 = con.prepareStatement(sql1);
ResultSet rs1 = pstmt1.executeQuery(sql1);

String sql2 = "select * from Ex_sum_count1 ORDER BY sum_num DESC LIMIT 1; ";
PreparedStatement pstmt2 = con.prepareStatement(sql2);
ResultSet rs2 = pstmt2.executeQuery(sql2);

String sql3 = "select * from Ex1_sum_count1 ORDER BY sum_num DESC LIMIT 1; ";
PreparedStatement pstmt3 = con.prepareStatement(sql3);
ResultSet rs3 = pstmt3.executeQuery(sql3);

String sql4 = "select * from Ex2_sum_count1 ORDER BY sum_num DESC LIMIT 1; ";
PreparedStatement pstmt4 = con.prepareStatement(sql4);
ResultSet rs4 = pstmt4.executeQuery(sql4);

%>
<!DOCTYPE html>

<html>
<head>
	  <meta http-equiv="refresh" content="600">
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <meta name="viewport" content="initial-scale=1, maximum-scale=1">
      <title>서울 월드컵 경기장</title>
      <meta name="keywords" content="">
      <meta name="description" content="">
      <meta name="author" content="">
      <link rel="stylesheet" href="css/bootstrap.min.css">
      <link rel="stylesheet" href="css/style.css">
      <link rel="stylesheet" href="css/responsive.css">
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
      <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.4.0/Chart.min.js"></script>
      <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
      
  <style type ='text/css' >
   #cameraview {
   display:none; /* 초기 상태에서는 보이지 않음 */
   width:480px; 
   height:480px; 
   position: absolute;
   top: 35.8%;
   left: 70%;
  }
  </style>    
  
</head>
 <body class="main-layout">
      <!-- loader  -->
      <div class="loader_bg">
         <div class="loader"><img src="images/loading.gif" alt="" /></div>
      </div>
<%-- 해더 시작 --%>
<header>
   <div class="header">
      <div class="container-fluid">
         <div class="row d_flex">
            <div class="col-md-2 col-sm-3 col logo_section">
               <div class="full">
                  <div class="center-desk">
                     <div class="logo">
                        <a href="main.jsp"><img src="images/logo.png" alt="#" /></a>
                     </div>
                  </div>
               </div>
            </div>
            <div class="col-md-8 col-sm-9">           
             <%-- 배너 색 설정하기 --%>
               <nav class="navigation navbar navbar-expand-md navbar-dark ">
                  <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarsExample04" aria-controls="navbarsExample04" aria-expanded="false" aria-label="Toggle navigation">
                     <span class="navbar-toggler-icon"></span>
                  </button>
                  <div class="collapse navbar-collapse" id="navbarsExample04">
                     <ul class="navbar-nav mr-auto">
                     <%-- 배너에 들어갈 아이템 설정 --%>
                        <li class="nav-item active">                   
                           <a class="nav-link" href="east.jsp">동쪽 출입구</a>
                        </li>
                        <li class="nav-item">
                           <a class="nav-link" href="west.jsp">서쪽 출입구</a>
                        </li>
                        <li class="nav-item">
                           <a class="nav-link" href="south.jsp">남쪽 출입구</a>
                        </li>
                        <li class="nav-item">
                           <a class="nav-link" href="north.jsp">북쪽 출입구</a>
                        </li>                      
                     </ul>
                  </div>
               </nav>
            </div>
         </div>
      </div>
   </div>
</header>
<%-- 해더 끝 --%>
<%-- 슬라이더 시작 --%>
	  <div id="top_section" class=" banner_main">
         <div class="container">
            <div class="row">
               <div class="col-md-12">
                  <div id="myCarousel" class="carousel slide" data-ride="carousel">
                     <ol class="carousel-indicators">
                        <li data-target="#myCarousel" data-slide-to="0" class="active"></li>
                        <li data-target="#myCarousel" data-slide-to="1"></li>
                        <li data-target="#myCarousel" data-slide-to="2"></li>
                        <li data-target="#myCarousel" data-slide-to="3"></li>
                     </ol>
                     <div class="carousel-inner">
                        <div class="carousel-item active">
                           <div class="container-fluid">
                              <div class="carousel-caption relative">
                                 <div class="bluid">
                                    <h1> 동문  </h1>
                                    <p> 동문의 현재 상황을 간편하게 볼 수 있어요. </p>                                    
                                    <a class="read_more" onclick="showPanel('동문')">간략히 보기 </a>
                                    <a class="read_more" href="east.jsp">자세히 보기 </a>
                                 </div>
                              </div>
                           </div>
                        </div>
                        <div class="carousel-item">
                           <div class="container-fluid">
                              <div class="carousel-caption relative">
                                 <div class="bluid">
                                    <h1> 서문  </h1>
                                    <p>  서문의 현재 상황을 간편하게 볼 수 있어요. </p>                                  
                                    <a class="read_more" onclick="showPanel('서문')">간략히 보기 </a>
                                    <a class="read_more" href="west.jsp">자세히 보기 </a>
                                 </div>
                              </div>
                           </div>
                        </div>
                        <div class="carousel-item">
                           <div class="container-fluid">
                              <div class="carousel-caption relative">
                                 <div class="bluid">
                                    <h1> 남문 </h1>
                                    <p> 남문의 현재 상황을 간편하게 볼 수 있어요
                                    </p>
                                    <a class="read_more" onclick="showPanel('남문')">간략히 보기 </a>
                                    <a class="read_more" href="south.jsp">자세히 보기 </a>
                                 </div>
                              </div>
                           </div>
                        </div>
                        <div class="carousel-item">
                           <div class="container-fluid">
                              <div class="carousel-caption relative">
                                 <div class="bluid">
                                    <h1> 북문 </h1>
                                    <p> 북문의 현재 상황을 간편하게 볼 수 있어요.
                                    </p>
                                    <a class="read_more" onclick="showPanel('북문')">간략히 보기 </a>
                                    <a class="read_more" href="north.jsp">자세히 보기 </a>
                                 </div>
                              </div>
                           </div>
                        </div>
                     </div>
                     <a class="carousel-control-prev" href="#myCarousel" role="button" data-slide="prev">
                     <i class="fa fa-angle-left" aria-hidden="true"></i>
                     <span class="sr-only">Previous</span>
                     </a>
                     <a class="carousel-control-next" href="#myCarousel" role="button" data-slide="next">
                     <i class="fa fa-angle-right" aria-hidden="true"></i>
                     <span class="sr-only">Next</span>
                     </a>
                  </div>
               </div>
            </div>
         </div>
      </div>
      
      <div id="right-side-panel" style="display: none; position: absolute; bottom: 0; right: 0; width: 40%; height: 88.5%; background-color: #f5f5f5; overflow-y: auto;">
        <video id="video1" width="100%" height="70%" autoplay></video>
        <p id="data-display"></p>
      </div>
      
      <script src="js/jquery.min.js"></script>
      <script src="js/bootstrap.bundle.min.js"></script>
      <script src="js/jquery-3.0.0.min.js"></script>
      <script src="js/custom.js"></script>
      
      <style>
		.circle {
    		display: inline-block;
    		width: 20px;
    		height: 20px;
    		border-radius: 50%;
		}

		.green {
    		background-color: green;
		}

		.blue {
    		background-color: blue;
		}

		.yellow {
    		background-color: yellow;
		}

		.orange {
    		background-color: orange;
		}

		.red {
    		background-color: red;
		}
		.flex-container {
  			display: flex;
  			align-items: center;
  			gap: 10px;
			}
		</style>
      <%
		String esnc = "", wsnc = "", ssnc = "", nsnc = "";

		if (rs1.next()) { esnc = rs1.getString("esnc"); }
		if (rs2.next()) { wsnc = rs2.getString("wsnc"); }
		if (rs3.next()) { ssnc = rs3.getString("ssnc"); }
		if (rs4.next()) { nsnc = rs4.getString("nsnc"); }
		%>

		<!-- esnc 값을 저장할 요소 -->
		<span id="esnc" style="display: none;"><%=esnc%></span>
		<span id="wsnc" style="display: none;"><%=wsnc%></span>
		<span id="ssnc" style="display: none;"><%=ssnc%></span>
		<span id="nsnc" style="display: none;"><%=nsnc%></span>
	  <script>
	  
	  function showPanel(location) {
		  var rightPanel = document.getElementById('right-side-panel');
		  var videoElement = document.getElementById('video1');
		  
		  var deviceId;
		  if (location === '동문') {
		    deviceId = 'de63f47ef5e4c8005ffc8ee4f168c23217269a97ea3855beed1fa3e1bc8917cc'; // 동문 카메라 deviceId
		  } else if (location === '서문') {
		    deviceId = 'de63f47ef5e4c8005ffc8ee4f168c23217269a97ea3855beed1fa3e1bc8917cc'; // 서문 카메라 deviceId
		  } else if (location === '남문') {
		    deviceId = 'de63f47ef5e4c8005ffc8ee4f168c23217269a97ea3855beed1fa3e1bc8917cc'; // 서문 카메라 deviceId
		  } else if (location === '북문') {
		    deviceId = 'de63f47ef5e4c8005ffc8ee4f168c23217269a97ea3855beed1fa3e1bc8917cc'; // 서문 카메라 deviceId
		  }
		  var esncElement;
		  if (location === '동문') {
		    esncElement = document.getElementById('esnc');
		  } else if (location === '서문') {
		    esncElement = document.getElementById('wsnc');
		  } else if (location === '남문') {
		    esncElement = document.getElementById('ssnc');
		  } else if (location === '북문') {
		    esncElement = document.getElementById('nsnc');
		  }
		  
		  var oldTextElement = rightPanel.querySelector('.text');
		  if (oldTextElement) {
		    rightPanel.removeChild(oldTextElement);
		  }
		  var oldCircleElement = rightPanel.querySelector('.circle');
		  if (oldCircleElement) {
		    rightPanel.removeChild(oldCircleElement);
		  }
		  
		  var circleElement = document.createElement('div');
		  circleElement.className = 'circle'; // 원 모양 클래스 추가 
		  
		  if (esncElement.textContent === '매우 원활') {
		    circleElement.classList.add('green');
		  } else if (esncElement.textContent === '원활') {
		    circleElement.classList.add('blue');
		  } else if (esncElement.textContent === '보통') {
		    circleElement.classList.add('yellow');
		  } else if (esncElement.textContent === '혼잡') {
		    circleElement.classList.add('orange');
		  } else if (esncElement.textContent === '매우 혼잡') {
		    circleElement.classList.add('red');
		  }
		  var textElement = document.createTextNode('현재 혼잡도 : ');
		  rightPanel.appendChild(textElement);
		  rightPanel.appendChild(circleElement);
		  
		  var brElement = document.createElement('br'); // 줄 바꿈 요소를 생성
		  rightPanel.appendChild(brElement);

		  var colors = ['green', 'blue', 'yellow', 'orange', 'red']; // 표시할 색상의 배열
		  var colorNames = ['매우 원활 :', '원활 :', '보통 : ', '혼잡 : ', '매우 혼잡 : ']; // 각 색상에 대응하는 텍스트의 배열

		  for (var i = 0; i < colors.length; i++) { // 색상의 배열을 순회
		    var containerElement = document.createElement('div'); // 컨테이너 요소를 생성
		    containerElement.className = 'flex-container';
		    
		    var textElement = document.createElement('p'); // p 요소를 생성
		    textElement.textContent = colorNames[i]; // 각 색상에 대응하는 텍스트를 설정
		    containerElement.appendChild(textElement); // 텍스트 요소를 컨테이너 요소에 추가
		    
		    var circleElement = document.createElement('div'); // div 요소를 생성
		    circleElement.className = 'circle ' + colors[i]; // 원과 각 색상을 표현하는 클래스를 추가
		    containerElement.appendChild(circleElement); // 원 요소를 컨테이너 요소에 추가

		    rightPanel.appendChild(containerElement); // 컨테이너 요소를 패널에 추가
		  }
		  
		  // 패널이 현재 보이는 상태라면 패널을 숨김
		  if (rightPanel.style.display === 'block') {
			    rightPanel.style.display = 'none';
			    
			    if (videoElement.srcObject) {
			        videoElement.srcObject.getTracks().forEach(function(track) {
			          track.stop();
			        });
			        videoElement.srcObject = null;
			      }
			  }
			  // 패널이 현재 숨겨진 상태라면 패널에 값을 설정하고 패널을 보여줌
		   else {
			   showPanelContents(location, deviceId);
			  }
			}
	  function showPanelContents(location, deviceId) {
		  var rightPanel = document.getElementById('right-side-panel');
		  var videoElement = document.getElementById('video1');
		  
		  // ... 패널에 표시할 내용 설정 ...
		  
		  // 패널을 보여줌
		  rightPanel.style.display = 'block';
		  
		  // 카메라 활성화
		  navigator.mediaDevices.getUserMedia({
		    video: {
		      deviceId: { exact: deviceId }
		    }
		  })
		  .then(function(stream) {
		    videoElement.srcObject = stream;
		  })
		  .catch(function(err) {
		    console.log(location + " 카메라 오류 발생: " + err);
		  });
		}
	  </script>
      
</body>

</html>
