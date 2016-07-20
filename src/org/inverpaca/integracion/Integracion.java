package org.inverpaca.integracion;

import java.io.IOException;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.dbcp.BasicDataSource;
import org.inverpaca.integracion.Integracion;


//import javax.naming.Context;
//import javax.naming.InitialContext;
//import javax.sql.DataSource;

//import javax.naming.Context;
//import javax.naming.InitialContext;
//import javax.sql.DataSource;


public class Integracion extends Conecciones {
  //Constructor
	public Integracion(){
		
	}
	//Variables seran utilizadas para capturar mensajes de errores de Oracle
	private String msj = null;
	//Variables para select
	private int columns;
	private String[][] arr;
	private int rows;
	private String[][] tabla;
	int cuenta = 0;
	int i;
	java.util.Date fecact = new java.util.Date();
	java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("dd/MM/yyyy hh:mm:ss aaa");
    String fecha = sdf.format(fecact);
    
    java.util.Date fechadiaD = new java.util.Date();
	java.text.SimpleDateFormat sdf1 = new java.text.SimpleDateFormat("dd/MM/yyyy");
    String fechadia = sdf1.format(fecact);
    
   

	
	 /**
     * Inserta logs.
     **/
    void insertLog(String detfaz, String estatus, String area) {
        try {
        	//Context initContext = new InitialContext();
            //DataSource ds = (DataSource) initContext.lookup("jdbc/orabiz");
        	
        	BasicDataSource ds = new BasicDataSource();
	   		ds.setDriverClassName(getBi_driver());
	   		ds.setUrl(getBi_url());
	   		ds.setUsername(getBi_usuario());
	   		ds.setPassword(getBi_clave());
	   		ds.setMaxActive(-1);
		  	ds.setMaxActive(-1);
	   		
	   		
            Connection con = ds.getConnection();
            PreparedStatement pstmt = null;
            //Class.forName(getDriver());
            //con = DriverManager.getConnection(
              //    getUrl(), getUsuario(), getClave());
            String query = "INSERT INTO BIAUDIT VALUES ('" + fecha + "','" + detfaz + "','" + estatus + "','" + area + "',TO_DATE('" + fechadia + "','dd/MM/yyyy'),'1')";
            pstmt = con.prepareStatement(query);
            System.out.println(query);
            try {
                //Avisando
                pstmt.executeUpdate();
                System.out.println("Se ha insertado registro en el log");
            } catch (SQLException e)  {
                msj =   e.getMessage();
            }
            
            pstmt.close();
            con.close();
        } catch (Exception e) {
        }
    }
    				
	/**
     * Lee datos de ZFI_ANALISISCOST
     **/
	private void  selectZFI_ANALISISCOST() {
		//System.out.println("entre al select");
       try {
           Statement stmt;
           ResultSet rs = null;
           //Context initContext = new InitialContext();
           //DataSource ds = (DataSource) initContext.lookup("jdbc/TUBDER03");
           BasicDataSource ds = new BasicDataSource();
	  	   ds.setDriverClassName(getSybase_driver());
	  	   ds.setUrl(getSybase_url());
	  	   ds.setUsername(getSybase_usuario());
	  	   ds.setPassword(getSybase_clave());
	  	   ds.setMaxActive(-1);
	  	   ds.setMaxActive(-1);
	  	   
  		   		 
           Connection con = ds.getConnection();
           stmt = con.createStatement(
              		ResultSet.TYPE_SCROLL_INSENSITIVE,
                    	ResultSet.CONCUR_READ_ONLY);
					   String query  = "SELECT ";
					   query += "ZFI_ANALISISCOST.MANDANTE "; // Mandante
					   query += ",ZFI_ANALISISCOST.ERDAT ";    // Fecha de registro
					   query += ",ZFI_ANALISISCOST.ERZET ";    // Hora de registro
					   query += ",ZFI_ANALISISCOST.AUFNR ";    // Numero de Orden
					   query += ",ZFI_ANALISISCOST.AUART ";    // Clase de orden
					   query += ",ZFI_ANALISISCOST.WERKS ";    // Centro
					   query += ",ZFI_ANALISISCOST.MATNR ";    // Numero del material
					   query += ",ZFI_ANALISISCOST.GSTRP ";    // FECHA DE INICIO EXTREMA 
					   query += ",ZFI_ANALISISCOST.INDIN ";    //
					   query += ",CASE ";
					   query += "   WHEN ZFI_ANALISISCOST.GLTRI ='00000000' THEN '20551231'";    // FECHA FIN REAL 
					   query += "   ELSE ZFI_ANALISISCOST.GLTRI ";
					   query += " END GLTRI ";
					   query += ",ZFI_ANALISISCOST.SORTB ";    //
					   query += ",ZFI_ANALISISCOST.BEWEG ";    // Operación empresarial en órdenes de producción
					   query += ",DD07V.DDTEXT";  // Texto de operación empresarial
					   query += ",ZFI_ANALISISCOST.HERKU ";    // 
					   query += ",ZFI_ANALISISCOST.KTEXT ";    // Texto breve p.característica KKBCS
					   query += ",ZFI_ANALISISCOST.MEINH ";    // Unidad de medida de la cantidad de producción
					   query += ",ZFI_ANALISISCOST.TWAER ";    // Clave de moneda
					   query += ",ZFI_ANALISISCOST.MEGXP ";    // Cantidad plan total
					   query += ",ZFI_ANALISISCOST.WTMGP ";    // Valor total en moneda de transacción
					   query += ",ZFI_ANALISISCOST.WTGXP ";    // Total de costes plan
					   query += ",ZFI_ANALISISCOST.MEGXR ";    // Cantidad real total
					   query += ",ZFI_ANALISISCOST.WTMGR ";    // Valor total en moneda de transacción
					   query += ",ZFI_ANALISISCOST.WTGXR ";    // Total de costes reales
					   query += ",ZFI_ANALISISCOST.PKSTA ";    // Desviación de costes plan/real absoluta
					   query += ",ZFI_ANALISISCOST.PKSTP ";    // Desviación de costes plan/real en porcentaje
					   query += "FROM R3P.SAPSR3.ZFI_ANALISISCOST ZFI_ANALISISCOST ";
					   query += "LEFT OUTER JOIN R3P.SAPSR3.DD07V DD07V ON ZFI_ANALISISCOST.BEWEG = DD07V.DOMVALUE_L ";
					   query += "                                          AND DD07V.DDLANGUAGE = 'S' ";
					   query += "                                          AND DD07V.DOMNAME = 'KKB_BEWEG' "; 
					   query += "WHERE ZFI_ANALISISCOST.GSTRP >= CONVERT(VARCHAR,DATEADD(dd,-45,GETDATE()),112) ";

					   //System.out.println(query);
           
					   try{
           rs = stmt.executeQuery(query);
           rows = 1;
		    rs.last();
		    rows = rs.getRow();
           System.out.println("Cantidad de Registros:" + rows);

           ResultSetMetaData rsmd = rs.getMetaData();
       	   columns = rsmd.getColumnCount();
		   System.out.println("Cantidad de Columnas:" +columns);
       	   arr = new String[rows][columns];
       	   
           int i = 0;
		    rs.beforeFirst();
           while (rs.next()){
               for (int j = 0; j < columns; j++)
				arr [i][j] = rs.getString(j+1);
                i++;
               /*System.out.println(arr[i][0]+","+arr[i][1]+","+arr[i][2]+","+arr[i][3]+","+arr[i][4]+","+arr[i][5]+","+arr[i][6]+","+arr[i][7]+","+arr[i][8]+","+arr[i][9]+","+arr[i][10]+","+arr[i][11]+","+arr[i][12]+","+arr[i][13]+","+
				                  arr[i][14]+","+
				                  arr[i][15]+","+
				                  arr[i][16]+","+
				                  arr[i][17]+","+
				                  arr[i][18]+","+
				                  arr[i][19]+","+
				                  arr[i][20]+","+
				                  arr[i][21]+","+
				                  arr[i][22]+","+
				                  arr[i][23]+","+
				                  arr[i][25]+"."); 
               */
				
              }
                   } catch (SQLException e) {
                   e.printStackTrace();
                   insertLog("ZFI_ANALISISCOST: " + e.getMessage(),"error", "ZFI_ANALISISCOST");
               }
           stmt.close();
           con.close();
           rs.close();

       } catch (Exception e) {
           e.printStackTrace();
           msj  = e.getMessage();
           insertLog("ZFI_ANALISISCOST: " + e.getMessage(),"error", "ZFI_ANALISISCOST");
       }
   }
	
	/**
     * Inserta registros utiliza multithread 100 hilos
     **/
	public void insertDatos()  {
		//Borra registros
		System.out.println("Entre al metodo insertDatos");
		
        BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(getBi_driver());
		ds.setUrl(getBi_url());
		ds.setUsername(getBi_usuario());
		ds.setPassword(getBi_clave());
		ds.setMaxActive(-1);
		ds.setMaxActive(-1);
        ExecutorService ex = Executors.newFixedThreadPool(10);   //Numero de hilos a usar para el insert
        
	    try {
	    	
	    Integracion q = new Integracion();
		
	    String
	    qryDelete = "DELETE ZFI_ANALISISCOST WHERE ERDAT >= SYSDATE -45 ";
	    
	    QueryGenericThread thd;
        thd = new QueryGenericThread(qryDelete, ds); //Insert Generic
        ex.execute(thd);
        Thread.sleep(1);
        System.out.println("DELETE EJECUTADO.");
	    
	    q.selectZFI_ANALISISCOST();
        tabla = q.getArr();
        rows = q.getRows();
	    
        //Inicia el Ciclo For
	    for (int i = 0; i < rows; i++) {
	    String 
    	qryInsert  = "INSERT INTO ZFI_ANALISISCOST values (" 
				+ "'" + tabla[i][0] + "', "
				+ "TO_DATE('" + tabla[i][1] + "','YYYYMMDD'), "
				+ "'" + tabla[i][2] + "', "
				+ "'" + tabla[i][3] + "', "
				+ "'" + tabla[i][4] + "', "
				+ "'" + tabla[i][5] + "', "
				+ "'" + tabla[i][6] + "', "
				+ "TO_DATE('" + tabla[i][7] + "','YYYYMMDD'), "
				+ "'" + tabla[i][8] + "', "
				+ "TO_DATE('" + tabla[i][9] + "','YYYYMMDD'), "
				+ "'" + tabla[i][10] + "', "
				+ "'" + tabla[i][11] + "', "
				+ "'" + tabla[i][12] + "', "
				+ "'" + tabla[i][13] + "', "
				+ "'" + tabla[i][14].replace("'", "") + "', "
				+ "'" + tabla[i][15] + "', "
				+ "'" + tabla[i][16] + "', "
				+ tabla[i][17] + ", "
				+ tabla[i][18] + ", "
				+ tabla[i][19] + ", "
				+ tabla[i][20] + ", "
				+ tabla[i][21] + ", "
				+ tabla[i][22] + ", "
				+ tabla[i][23] + ", "
				+ tabla[i][24] + ")";
        
    	 //System.out.println(qryInsert);    			     		
	             
       	 QueryGenericThread th;
         th = new QueryGenericThread(qryInsert, ds); //Insert Generic
         ex.execute(th);
         Thread.sleep(1);
         
	    } // Fin del loop for
     
	    if(rows==0){
        msj = "ZFI_ANALISISCOST" + " : Lectura de datos no realizada, no se encontraron registros";
        insertLog(msj, "error", "ZFI_ANALISISCOST");
        } else {
        msj = "ZFI_ANALISISCOST" + " : Registros insertados con exito. " + rows + " registros";
        insertLog(msj, "exito", "ZFI_ANALISISCOST");
        }
        
	        ds.close();
	} catch (Exception e) {
	        insertLog(e.getMessage(),"error", "ZFI_ANALISISCOST");
	    }
	    
	}
  /**
	 * @return the arr
	 */
	public String[][] getArr() {
		return arr;
	}


	/**
	 * @return the rows
	 */
	public int getRows() {
		return rows;
	}

	

     /**
	 * @return the msj
	 */
	public String getMsj() {
		return msj;
	}
	

	//Ejecuta el programa
	public static void main (String args []) throws IOException{
	//System.out.println("aqui inicio");
	Integracion a = new Integracion();
	//Ejecuta TUBDER03
	
	System.out.println("Iniciando Interfaz");
	a.insertDatos();
	System.out.println("FIN ZFI_ANALISISCOST");
	
     {
        System.exit(0);
    }
	            
	}
	
}