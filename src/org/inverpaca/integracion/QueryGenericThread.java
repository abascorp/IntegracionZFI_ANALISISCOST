/**
 * 
 */
package org.inverpaca.integracion;
/**
 * 
 */


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;



/**
 * @author andres
 * Clase para generar querys genéricos utiliando los hilos de la computadora
 *
 */
public class QueryGenericThread extends Thread {

	/**
	 * 
	 */
	String query;
	DataSource dataSource = null;
	PreparedStatement pstmt = null;
	
	public QueryGenericThread(String query, DataSource dataSource) {
		this.query = query;
		this.dataSource = dataSource;
	}

	
	public void run() {	
		Connection conexion = null;
	      try {
	         conexion = dataSource.getConnection();
	         pstmt = conexion.prepareStatement(query);
	            //System.out.println(query);
	            
	        pstmt.executeUpdate();
	         
	      } catch (Exception e) {
	         // tratamiento de error
	    	  System.out.println("Error en Thread : " + e.getMessage());
	    	  new Integracion().insertLog("Error en Thread - " + e.getMessage(),"error", "ZFI_ANALISISCOST");
	    	  System.exit(0);
	    	  
	      } finally {
	         if (null != conexion)
				try {
					conexion.close();
					pstmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	      }
		  
	   }

}
