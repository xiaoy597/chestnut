package com.evergrande.bigdata.utils

import java.sql.{ResultSet, PreparedStatement, DriverManager, Connection}

/**
 * Created by lingxiao on 2016/5/10.
 */
object JDBCUtils {


  def getConn() : Connection = {
    var conn: Connection = null

    try {
      Class.forName("com.mysql.jdbc.Driver");
//      conn = DriverManager.getConnection("jdbc:mysql://172.16.52.120:3306/app_config", "app_config", "123456")

      // Connect to MySQL server in IDC
      conn = DriverManager.getConnection("jdbc:mysql://10.127.133.202:3306/user_profile", "dmp_admin", "0w*4XEdhn")

    } catch {
      case e: Exception => e.printStackTrace()
    }
    conn
  }

  def closeConn(rs : ResultSet, pstmt : PreparedStatement, conn: Connection) : Unit = {
    if(rs != null){
      try {
        rs.close();
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    if(pstmt != null){
      try {
        pstmt.close();
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    if(conn != null){
      try {
        conn.close();
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  //用于发送增删改语句的方法
  def execOther(ps : PreparedStatement) : Int = {
    var affectedRows = -1
    try {
      //1、使用Statement对象发送SQL语句
      affectedRows = ps.executeUpdate();
    } catch {
      case e: Exception => e.printStackTrace()
    }
    affectedRows
  }


  //method4: 专门用于发送查询语句
  def execQuery(ps : PreparedStatement) : ResultSet = {
    var rs : ResultSet = null
    try {
      //1、使用Statement对象发送SQL语句
      rs = ps.executeQuery();
      //2、返回结果
      rs;
    } catch {
      case e: Exception => e.printStackTrace()
        null
    }
  }
}
