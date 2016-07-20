package org.inverpaca.integracion;

public class Conecciones {
	//Constructor
	public Conecciones(){
		
	}
	
	//Variables para coneccion con BD BizView
    private String bi_usuario = "openbizview";
    private String bi_clave = "openbizview";
    private String bi_url = "jdbc:oracle:thin:@10.10.7.10:1521:bizview";
    private String bi_driver = "oracle.jdbc.OracleDriver"; //Driver
    
    //Variables para coneccion con BD Sybase
    private String sybase_usuario = "sapsa";
    private String sybase_clave = "*.Tpvc2804";
    private String sybase_url = "jdbc:jtds:sybase://10.10.2.101:4901:R3P";
    private String sybase_driver = "net.sourceforge.jtds.jdbc.Driver"; //Driver
	public String getBi_usuario() {
		return bi_usuario;
	}
	public String getBi_clave() {
		return bi_clave;
	}
	public String getBi_url() {
		return bi_url;
	}
	public String getBi_driver() {
		return bi_driver;
	}
	public String getSybase_usuario() {
		return sybase_usuario;
	}
	public String getSybase_clave() {
		return sybase_clave;
	}
	public String getSybase_url() {
		return sybase_url;
	}
	public String getSybase_driver() {
		return sybase_driver;
	}
	
    
}
