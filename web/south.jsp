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
String sql = "SELECT FLOOR((sum_num - 1) / 3600) as group_num, AVG(sum_count) as avg_sum_count FROM Ex1_sum_count GROUP BY group_num LIMIT 6 OFFSET 1;";
PreparedStatement pstmt = con.prepareStatement(sql);
ResultSet rs = pstmt.executeQuery(sql);

// 데이터를 리스트로 만들기
List<String> data = new ArrayList<String>();
while(rs.next()) {
	String groupNum = String.valueOf(rs.getInt("group_num"));
	data.add("['" + groupNum + "0분', " + rs.getInt("avg_sum_count") + "]");
}

String sql1 = "select * from Ex1_sum_count1 ORDER BY sum_num DESC LIMIT 1; ";
PreparedStatement pstmt1 = con.prepareStatement(sql1);
ResultSet rs1 = pstmt1.executeQuery(sql1);

String sql2 = "select * from south_cal ORDER BY south_num DESC LIMIT 1;";
PreparedStatement pstmt2 = con.prepareStatement(sql2);
ResultSet rs2 = pstmt2.executeQuery(sql2);

String sql3 = "select * from move ORDER BY move_num DESC LIMIT 1, 1;";
PreparedStatement pstmt3 = con.prepareStatement(sql3);
ResultSet rs3 = pstmt3.executeQuery(sql3);

String sql4 = "select * from distance ORDER BY num DESC LIMIT 1;";
PreparedStatement pstmt4 = con.prepareStatement(sql4);
ResultSet rs4 = pstmt4.executeQuery(sql4);

%>
<!DOCTYPE html>

<html>
   <head>
      <meta http-equiv="refresh" content="600">
      <meta charset="utf-8">
      <meta http-equiv="X-UA-Compatible" content="IE=edge">
     
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <meta name="viewport" content="initial-scale=1, maximum-scale=1">
     
      <title>남쪽 출입구</title>
      <meta name="keywords" content="">
      <meta name="description" content="">
      <meta name="author" content="">    
      <link rel="stylesheet" href="css/bootstrap.min.css">     
      <link rel="stylesheet" href="css/style.css">    
      <link rel="stylesheet" href="css/responsive.css">   
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
      <script src="https://oss.maxcdn.com/html5shiv/3.7.3/html5shiv.min.js"></script>
      <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
   </head>
   <body class="main-layout inner_page">
      <div class="loader_bg">
         <div class="loader"><img src="images/loading.gif" alt="" /></div>
      </div>
      <header>
         <div class="header">
            <div class="container-fluid">
               <div class="row d_flex">
                  <div class=" col-md-2 col-sm-3 col logo_section">
                     <div class="full">
                        <div class="center-desk">
                           <div class="logo">
                              <a href="main.jsp"><img src="images/logo.png" alt="#" /></a>
                           </div>
                        </div>
                     </div>
                  </div>
                  <div class="col-md-8 col-sm-9">
                     <nav class="navigation navbar navbar-expand-md navbar-dark ">
                        <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarsExample04" aria-controls="navbarsExample04" aria-expanded="false" aria-label="Toggle navigation">
                        <span class="navbar-toggler-icon"></span>
                        </button>
                        <div class="collapse navbar-collapse" id="navbarsExample04">
                           <ul class="navbar-nav mr-auto">
                              <li class="nav-item active">                   
                           <a class="nav-link" href="east.jsp">동쪽 출입구</a>
                        </li>
                        <li class="nav-item">
                           <a class="nav-link" href="west.jsp">서쪽 출입구</a>
                        </li>
                        <li class="nav-item">
                           <a class="nav-link" href="soust.jsp">남쪽 출입구</a>
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
      <div class="portfolio">
         <div class="container">
            <div class="row">
               <div class="col-md-12">
                  <div class="titlepage text_align_left">
                     <h2> 남쪽 출입구 현황  </h2>
                  </div>
               </div>
            </div>
            <div class="row">
               <div class="col-md-6">
                  <div id="ho_nf" class="portfolio_main text_align_left" style="background-color: #E8E8E8; color: #000;">
                     <figure>
                        <img src="월드컵 경기장.png">
                        <div class="portfolio_text" style="background-color: #E8E8E8;">
                           <div style="text-align: center;">
        				   	    <h3>실시간 혼잡도 정보</h3>
    					   </div>
                           <%
								while (rs1.next()) {
								%>
								<% String ssnc = rs1.getString("ssnc"); %>
								실시간 혼잡도 : <%=ssnc%>
								<div style="display:inline-block;width:20px;height:20px;border-radius:50%;margin-left:10px;
								<% 
								if(ssnc.equals("매우 원활")) { 
    								out.print("background-color:green;"); 
								} else if(ssnc.equals("원활")) { 
    								out.print("background-color:blue;"); 
								} else if(ssnc.equals("보통")) { 
    								out.print("background-color:yellow;"); 
								} else if(ssnc.equals("혼잡")) { 
    								out.print("background-color:orange;"); 
								} else if(ssnc.equals("매우 혼잡")) { 
    								out.print("background-color:red;"); 
								} 
								%>
								"></div><br>
								대기 인원 : <%=rs1.getString("s_count")%>명, 
								대기 시간 : <%=rs1.getString("wait")%>분 <br><br>
								<%
								}
								%>
					
								<%
								while (rs4.next()) {
								%>
								이동 소요 시간 <br>
								동쪽 : <%=rs4.getString("next")%>분 
								서쪽 : <%=rs4.getString("next")%>분 
								북쪽 : <%=rs4.getString("opp")%>분<br>
								<%
								}
								%>
								
								<%
								while (rs2.next()) {
								%>
								추천 출입구 : <%=rs2.getString("srec")%>합니다<br>
								<%
								}
								%>
                        </div>
                     </figure>
                  	</div>
               	</div>
               <div class="col-md-6">
                  <div id="ho_nf" class="portfolio_main text_align_left" style = "color: #000;">
                     <figure>
                        <img src="images/p1234.png">
                        <div class="portfolio_text" style="background-color: #E8E8E8;">
                           <h3>남쪽 출입구 정보</h3>
                           <%
								while (rs3.next()) {
									String weather = rs3.getString("move_name");
								%>
									현재 날씨: <%= weather %>
									<% if (weather.equals("맑음")) { %>
            							<a href="https://weather.naver.com/compare/09440101" target="_blank">
       										<img src="맑음.png">
      									</a>
        							<% } else if (weather.equals("흐림")) { %>
            							<a href="https://weather.naver.com/compare/09440101" target="_blank">
       										<img src="흐림.png">
      									</a>
        							<% } else if (weather.equals("눈")) { %>
            							<a href="https://weather.naver.com/compare/09440101" target="_blank">
       										<img src="눈.png">
      									</a>
        							<% } else if (weather.equals("비")) { %>
            							<a href="https://weather.naver.com/compare/09440101" target="_blank">
       										<img src="비.png">
      									</a>
      								<% } else if (weather.equals("구름많음")) { %>
            							<a href="https://weather.naver.com/compare/09440101" target="_blank">
       										<img src="구름많음.png">
      									</a>
      								<% } else if (weather.equals("구름조금")) { %>
            							<a href="https://weather.naver.com/compare/09440101" target="_blank">
       										<img src="구름조금.png">
      									</a>
            						<% } %>
								<%
								}
								%>
							경기장 정보 : 
							<a href="https://map.naver.com/p/entry/place/11571843?lng=126.89727330000005&lat=37.56825529999948&placePath=%2F&entry=plt&c=15.00,0,0,0,dh" target="_blank">
       							<img src="네이버 지도.png">
      						</a>
                        </div>
                     </figure>
                  </div>
               </div>
               <div class="col-md-6">
                  <div id="ho_nf" class="portfolio_main text_align_left" style="background-color: #E8E8E8;">
                     <figure>
                        <div class="entrance">
                           <div id="chart_div" style="width:100%; height:100%;"></div>
                        </div>
                     </figure>
                  </div>
               </div>
               <div class="col-md-6">
                  <div id="ho_nf" class="portfolio_main text_align_left" style="background-color: #E8E8E8; color: #000;">
                     <figure>
                        <div class="entrance">
                        	<p>남쪽 카메라</p>
                        	<img src="http://127.0.0.1:5002/video_feed" />
                        </div>
                     </figure>
                  </div>
               </div>
            </div>
         </div>
      </div>
      <!-- end portfolio -->
      
      <!-- Javascript files-->
      <script src="js/jquery.min.js"></script>
      <script src="js/bootstrap.bundle.min.js"></script>
      <script src="js/jquery-3.0.0.min.js"></script>
      <script src="js/custom.js"></script>
      
      <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    				<script type="text/javascript">
   					google.charts.load('current', {'packages':['corechart']});
    				google.charts.setOnLoadCallback(drawChart);
    				function drawChart() {
        				var data = google.visualization.arrayToDataTable([
            				['group_num', '실시간        혼잡도'],
            				<% for(String s : data) { %>
                				<%= s %>,
            				<% } %>
        				]);
        				var options = {
        					'backgroundColor': '#E8E8E8',
        					'title':'남쪽 혼잡도 그래프', 
        					'width':500, 'height':400,
        					'hAxis':{
        						'viewWindow':{
        							'min': 0,
        							'max': 6
        						}
        					},
        					'vAxis':{
        						'viewWindow': {
        							'min': 0,
        							'max': 100
        						}
        					}
        			};
        			var chart = new google.visualization.LineChart(document.getElementById('chart_div'));
        			chart.draw(data, options);
    			}
    			</script>
   </body>
</html>