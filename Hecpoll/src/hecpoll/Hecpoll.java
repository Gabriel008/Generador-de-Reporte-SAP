/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hecpoll;
import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;
import java.net.InetAddress;

import org.apache.commons.net.ftp.FTPFile;
import java.io.*;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLConnection;
import java.security.AccessControlException;
import java.security.cert.X509Certificate;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.json.JSONException;
import org.json.JSONObject;

import java.nio.charset.Charset;

import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPReply;
import org.json.JSONArray;
import org.json.JSONStringer;
//import sun.invoke.util.ValueConversions;


/**
 *
 * @author Gabriel
 */
public class Hecpoll {
    /**
     * @param args the command line arguments
     */
    static String servername1;
    static String databaseName1; 
    static String user1;
    static String pass1;
    
    static String servername2;
    static String databaseName2; 
    static String user2;
    static String pass2;
    static String port2;
    
    static String servername3;
    static String databaseName3; 
    static String user3;
    static String pass3;
    
    static String sFTP;
    static String ftpUser;
    static String ftpPassword;
    
    static String rutaSap;
    static String remotePath;
    static String HoraS;
    static String MinutoS;
    static String IDPAYMEN;
    static String IDPAYMENini;
    
    static String estado;
    static String Linea;
    static String Linea2;
    static boolean existe=false;
    static boolean existeSap=false;
    static boolean existeSap1=false;
    static boolean Flag=false;
    //static boolean est=false;
    static int sta  =0;
    static Statement st;
    static ResultSet rs,rs1,rs2;

    static String idPaymen;
    static String AUXidPaymen;
    static String combustible;
    static String sitio;
    static String fecha;
    static String hora;
    static String trx;
    static String tipoTRX;
    static String maquina;
    static String litros;
    static String odometro;
    static String tipo;
    static String operador;
    static String numero;
    static String pistola;
    
    
    static int cont=0;
    
public static void main(String[] args) throws IOException, Exception{
// TODO code application logic here<
boolean _exit=false;
    while(!_exit)
    {
        BuscarINI();//Recupera informacion del archivo .ini
       // EliminaTXT();
        Calendar calendario = Calendar.getInstance();        
        int hora, minutos, segundos;
        hora =calendario.get(Calendar.HOUR_OF_DAY);
        minutos = calendario.get(Calendar.MINUTE);
        segundos = calendario.get(Calendar.SECOND);
        System.out.println("----------------------------------------------------------");
        System.out.println("HORA ACTUAL: "+hora + ":" + minutos + ":" + segundos);
        System.out.println("PROXIMA EJECUCION A LAS: "+HoraS + ":" + MinutoS );
        System.out.println("ULTIMO ID REGISTRADO :"+IDPAYMEN);
        if ((Integer.parseInt(HoraS)==hora)&&(minutos>=Integer.parseInt(MinutoS)))
        {            
            System.out.println("CUMPLE HORA, FLAG :"+Flag);
            if (Flag==false)
            {
               if(cont==0){
                   cont=1;
                Flag=true;
                System.out.println("CAMBIO FLAG :"+Flag);     
                Log.log("ES HORA DE EJECUTAR \n");
                Conexion1(); //SAP
                Conexion2(); // Oracle
                Conexion3(); //Hecpoll
                
               }
           }
        }
        else {
               if (hora==Integer.parseInt(HoraS)+1 ||hora>Integer.parseInt(HoraS)){
                   cont=0;
                   }    
            Flag=false;
            System.out.println("FUERA DE HORARIO FLAG: "+Flag+"\n");
            System.out.println("CONSULTANDO ESTADO JSON - FLAG: "+Flag+"\n");
            System.out.println("ESTADO JSON: "+estado);

                if (Integer.parseInt(estado)==1)
                {  
                    System.out.println("No cumple hora pero estado :"+estado+" Ejecutar - Flag :"+Flag+"\n");
                   
                        Conexion1();
                        Conexion2(); // Oracle
                        Conexion3();
                          
                }
                  if (Integer.parseInt(estado)==2)
                {  int cont=0;
                    System.out.println("No cumple hora pero estado :"+estado+" Ejecutar - Flag :"+Flag+"\n");
                    Conexion3();
                    cont++;
                    if(cont==1){
                        estado="1";
                        modificarEstado(Linea2);
                        cont=0;
                    }
                            
                        
                }
          }
        System.out.println("----------------------------------------------------------");
        Thread.sleep(15000);
    }     
 }  
       
    
public static void Conexion1() throws SQLException, Exception{  
Statement st1;
ResultSet rs1;
String files;
estado="1";
modificarEstado(Linea2);
try
{
    System.out.println("INTENTANDO CONEXION BD SAP");
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    Connection conn = null; 
    try{    
    //conn = DriverManager.getConnection("jdbc:sqlserver://;servername=DESKTOP-GN76FCS\\SQLEXPRESS;databaseName=SAP","sa","Micrologica2014");
    //String ConnectionURL="jdbc:sqlserver://172.20.4.157:1433;DatabaseName=SAP;user=SAP;Password=sap2016";
    //System.out.println(connUrl);
   // String connUrl="jdbc:sqlserver://"+servername1+":1433;DatabaseName="+databaseName1+";user="+user1+";Password="+pass1;
   // conn = DriverManager.getConnection(connUrl); 
      
    conn = DriverManager.getConnection("jdbc:sqlserver://"+servername1+":1433;DatabaseName="+databaseName1+";user="+user1+";Password="+pass1); 
    
    
    System.out.println("CONEXION BD SAP OK");
    Log.log("CONEXION BD SAP OK ");
    
    }
    catch (SQLException e)
               {
                   System.out.println ("ERROR CONEXION BD SAP");
                    Log.log("ERROR CONEXION BD SAP \n");
                  // String connUrl="jdbc:sqlserver://"+servername1+":1433;DatabaseName="+databaseName1+";user="+user1+";Password="+pass1;
                    e.printStackTrace();
                    Log.log(e.toString());
                    if (conn!=null)
                    conn.close();
                    int c = 0;
                    while (c==0)
                    {
                        try
                        { 
                        //conn = DriverManager.getConnection(connUrl);    
                        conn = DriverManager.getConnection("jdbc:sqlserver://"+servername1+":1433;DatabaseName="+databaseName1+";user="+user1+";Password="+pass1); 
                        Log.log("RECUPERA CONEXION DB SAP");
                        c=1;
                        }
                        catch (SQLException s)
                        {
                            System.out.println("ERR: "+s.toString());
                            Log.log("ERR: "+s.toString());
                        }
                        Thread.sleep(10000);
                    }
               }
    try{
        System.out.println("INICIO MA_SitioSAP.txt");  
        Log.log("INICIO MA_SitioSAP.txt");
        String Archivo1="MA_SitioSAP.txt";
         String pathSAP = new java.io.File(".").getCanonicalPath();
         boolean hay;
        
          hay= EliminaTXT(Archivo1);
          
           System.out.println("hay ARCHIVO MA_SitioSAP.txt? :"+hay );
          
        if(hay==false){
        System.out.println("RESCATANDO DATOS TBL MA_SitiosSAP");
        Log.log("RESCATANDO DATOS TBL MA_SitiosSAP");
        st1=conn.createStatement();                    
        String SQuery1="select SiSaSitioCombustible,SiSaSitioSAP from MA_SitioSAP order by SiSaid" ;
        rs1=st1.executeQuery(SQuery1);
        //System.out.println(pathSAP);
        while (rs1.next()){
          docSap.docSap(pathSAP, rs1.getObject(1).toString().trim()+",'"+rs1.getObject(2).toString().trim()+"'");            
        }
        conn.close();
        System.out.println("FIN MA_SitioSAP.txt");
         Log.log("FIN MA_SitioSAP.txt");
     }else{
          System.out.println("HA OCURRIDO ALGO");
          Log.log("OT Err: HA OCURRIDO ALGO");
          }
    }                
    catch(Exception e){
            e.printStackTrace();  
            }    
}
catch(Exception e)
{
    System.out.println("OT Err");
    Log.log("OT Err: "+e.toString());
    e.printStackTrace();
}        
 
 } 
  public static void Conexion2() throws SQLException{
    try
   {
    System.out.println("INTENTANDO CONEXION BD ORACLE");
     Log.log("INTENTANDO CONEXION BD ORACLE");
          Class.forName("oracle.jdbc.OracleDriver");
         //System.out.println("oracle.jdbc.OracleDriver");
    Connection connO = null; 
    try{
       //jdbc:oracle:thin:gxuser/gx.2017@172.20.4.178:1521:gxsuitop
      //System.out.println("jdbc:oracle:thin:"+user2+"/"+pass2+"@"+servername2+":"+port2+":"+databaseName2);
      connO = DriverManager.getConnection("jdbc:oracle:thin:"+user2+"/"+pass2+"@"+servername2+":"+port2+":"+databaseName2); 
    System.out.println("CONEXION BD ORACLE OK");    
    Log.log("CONEXION BD ORACLE OK");
    }
    catch (SQLException e)
               {
                   System.out.println("ERROR CONEXION BD ORACLE");
                   Log.log("ERROR CONEXION BD ORACLE");
                   e.printStackTrace();
                   Log.log("Err BD ORACLE: "+e.toString());
                   if (connO!=null)
                   connO.close();
                   int c = 0;
                   while (c==0)
                   {
                       try
                       {                      
                       //conn = DriverManager.getConnection("jdbc:sqlserver://172.20.4.220;databaseName=HECPOLL","sa","FleetAccess1$");
                       //conn = DriverManager.getConnection("jdbc:sqlserver://;servername=BOLSÑORES\\HECTRONIC;databaseName=HECPOLL","sa","FleetAccess1$");
                       connO = DriverManager.getConnection("jdbc:oracle:thin:"+user2+"/"+pass2+"@"+servername2+":"+port2+":"+databaseName2); 
                       Log.log("RECUPERA CONEXION BD ORACLE");
                       c=1;
                       }
                       catch (SQLException s)
                       {
                           System.out.println("Err: "+s.toString());
                           Log.log("Err BD ORACLE: "+s.toString());
                       }
                       Thread.sleep(10000);
                   }
               }
   
    
    try{
            System.out.println("INICIO rl_cambios_flota.txt");
            Log.log("INICIO rl_cambios_flota.txt");
            String files;
            Statement st1;
            ResultSet rs1;
            String pathOracle = new java.io.File(".").getCanonicalPath();
            String ArchivoOracle1="rl_cambios_flota.txt";
            String ArchivoOracle2="MA_flota.txt";
            boolean hay1;
            boolean hay2;
         
           hay1= EliminaTXT(ArchivoOracle1);
           
        if(hay1==false){
        System.out.println("RESCATANDO DATOS TBL rl_cambios_flota");
        Log.log("RESCATANDO DATOS TBL rl_cambios_flota");
        st1=connO.createStatement();                    
        String SQuery1="select CBFLID,nvl(CBFLTPOFCODIGO,0), CBFLFLTAIDINTERNO, CBFLFECHA,CBFLFLTAORDENCO,CBFLFLTAOPERADORSAP from OPRUTA.rl_cambios_flota" ;
        rs1=st1.executeQuery(SQuery1);
        System.out.println(pathOracle);
        
        while (rs1.next()){
          docOracle.docOracleRL(pathOracle, rs1.getObject(1).toString().trim()+","+rs1.getObject(2).toString().trim()+",'"+rs1.getObject(3).toString().trim()+"',convert(datetime,'"+rs1.getObject(4).toString().trim()+"',102),'"+rs1.getObject(5).toString().trim()+"','"+rs1.getObject(6).toString().trim()+"'" );            
        }
        System.out.println("FIN rl_cambios_flota.txt");
        Log.log("FIN rl_cambios_flota.txt");
        }else{
          System.out.println("HA OCURRIDO ALGO");
          Log.log("OT Err: HA OCURRIDO ALGO");
          }
        
        
        
        System.out.println("INICIO MA_flota.txt");
        Log.log("INICIO MA_flota.txt");
        
      
         hay2= EliminaTXT(ArchivoOracle2);
        if(hay2==false){
        System.out.println("RESCATANDO DATOS TBL MA_flota");
        Log.log("RESCATANDO DATOS TBL MA_flota");
        st1=connO.createStatement();                    
        String SQuery2="select TPOFCODIGO,nvl(FLTAIDINTERNO,0),nvl(FLTA_ORDEN_CO,'nulo'),nvl(FLTA_OPERADOR_SAP,'nulo') from OPRUTA.ma_flota" ;
        rs1=st1.executeQuery(SQuery2);
        //System.out.println(pathOracle);
        while (rs1.next()){
          docOracle.docOracleMA(pathOracle, rs1.getObject(1).toString().trim()+","+rs1.getObject(2).toString().trim()+",'"+rs1.getObject(3).toString().trim()+"','"+rs1.getObject(4).toString().trim()+"'");
        }       
        System.out.println("FIN MA_flota.txt");
        Log.log("FIN MA_flota.txt");
         }else{
          System.out.println("HA OCURRIDO ALGO");
          Log.log("OT Err: HA OCURRIDO ALGO");
          }

        connO.close();
    }                
    catch(Exception e){
            e.printStackTrace();  
            }
    
    }
    catch(Exception e)
    {
        System.out.println("OT Err");
        Log.log("OT Err: "+e.toString());
        e.printStackTrace();
    }
}
 public static void Conexion3() throws SQLException,Exception{
    Statement st1;
    ResultSet rs1;
    boolean _Flag=false;
    estado="2";
    modificarEstado(Linea2);
     try
        {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection conn = null;
        System.out.println("INICIO PROCESO HECPOLL");
         Log.log("INICIO PROCESO HECPOLL");
        while (_Flag==false)
        {
            try
            {
                //conn = DriverManager.getConnection("jdbc:sqlserver://;servername=BOLSÑORES\\HECTRONIC;databaseName=HECPOLL","sa","FleetAccess1$");            
                // conn = DriverManager.getConnection("jdbc:sqlserver://172.20.4.220;databaseName=HECPOLL","sa","FleetAccess1$");
                //conn = DriverManager.getConnection("jdbc:sqlserver://172.24.6.84;databaseName=HECPOLL","sa","FleetAccess1$");
                //conn = DriverManager.getConnection("jdbc:sqlserver://172.24.6.84;databaseName=HECPOLL","sa","FleetAccess1$");
                conn = DriverManager.getConnection("jdbc:sqlserver://"+servername3+":1433;DatabaseName="+databaseName3+";user="+user3+";Password="+pass3); 
                 Log.log("CONEXION BD HECPOLL OK");
            }
            catch (SQLException e)
            {
                e.printStackTrace();
                Log.log("Err BD HECPOLL: "+e.toString());
                if (conn!=null)
                conn.close();
                int c = 0;
                while (c==0)
                {
                    try
                    {
                    //conn = DriverManager.getConnection("jdbc:sqlserver://;servername=BOLSÑORES\\HECTRONIC;databaseName=HECPOLL","sa","FleetAccess1$");
                    //conn = DriverManager.getConnection("jdbc:sqlserver://172.20.4.220;databaseName=HECPOLL","sa","FleetAccess1$");
                    //conn = DriverManager.getConnection("jdbc:sqlserver://172.24.6.84;databaseName=HECPOLL","sa","FleetAccess1$");
                     //conn = DriverManager.getConnection("jdbc:sqlserver://172.24.6.84;databaseName=HECPOLL","sa","FleetAccess1$");
                    conn = DriverManager.getConnection("jdbc:sqlserver://"+servername3+":1433;DatabaseName="+databaseName3+";user="+user3+";Password="+pass3); 
                  
                   
                    Log.log("RECUPERA CONEXION BD HECPOLL");
                    c=1;
                    }
                    catch (SQLException s)
                    {
                        Log.log("Err BD HECPOLL: "+e.toString());
                       
                    }
                    Thread.sleep(60000);
                }
            }
            try
            {
                System.out.println("CONEXION HECPOLL OK");
//*************************************************************************************************************************                
//************************************Procesar archivo MA_SitioSAP.txt*****************************************************
//*************************************************************************************************************************
                                    
                String SQuery1="select * from sys.tables where name='MA_SitioSAPTemp'" ;
                st1 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                rs1=st1.executeQuery(SQuery1);
                //System.out.println(pathSAP);
                rs1.last();
                if (rs1.getRow()>0){
                    System.out.println("TABLA MA_SitioSAPTemp EXISTE"); 
                    st1 = conn.createStatement();
                    st1.executeUpdate("delete from MA_SitioSAPTemp");
                    System.out.println("TABLA MA_SitioSAPTemp BORRADA"); 
                     
                }else{
                    System.out.println("TABLA MA_SitioSAPTemp NO EXISTE");                                         
                    st1=conn.createStatement();
                    st1.executeUpdate("create table MA_SitioSAPTemp(SiSaSitioCombustible smallint not null,SiSaSitioSAP char(4)  not null)");
                    System.out.println("TABLA MA_SitioSAPTemp CREADA"); 
                }
                    
                
                boolean ArchivoSAP=false;
                String NomArchivo = "Hecpoll_ini.txt";
                String path = new java.io.File(".").getCanonicalPath();
                String files;
                FileReader fr=null;                
                File folder = new File(path);
                File[] listOfFiles = folder.listFiles(); 
            //Buscar arhivo que termina en ini.txt
                for (int i = 0; i < listOfFiles.length; i++) 
                {
                    if (listOfFiles[i].isFile()) 
                    {
                        files = listOfFiles[i].getName();
                        if (files.endsWith("MA_SitioSAP.txt"))
                            {
                                //System.out.println(files);
                                ArchivoSAP=true;
                                break;
                            }else{
                            ArchivoSAP=false;
                        }
                    }
                }
                if (ArchivoSAP=true){                    
                    try {
                        String SQuery="";    
                        String pathSAP = new java.io.File(".").getCanonicalPath();
                        System.out.println("PROCESANDO ARCHIVO MA_SITIOSAP.txt");   
                        Log.log("PROCESANDO ARCHIVO MA_SITIOSAP.txt");
                        fr = new FileReader (pathSAP+"\\MA_SitioSAP.txt");
                        BufferedReader br = new BufferedReader(fr);
                        String Registro="";
                        while((Linea=br.readLine())!=null){
                            //recuperar() ;
                            Registro=Linea;                    
                            //System.out.println(Registro);                     
                            SQuery="insert into MA_SitioSAPTemp values ("+Registro+")";
                            //System.out.println(SQuery);
                            st1=conn.createStatement();
                            st1.executeUpdate(SQuery);                     
                        } 
                    } catch(Exception e){
                    e.printStackTrace();
                    }finally{
                    // En el finally cerramos el fichero, para asegurarnos
                    // que se cierra tanto si todo va bien como si salta 
                    // una excepcion.
                    try{                    
                    if( null != fr ){   
                       fr.close();     
                    }                  
                    }catch (Exception e2){ 
                    e2.printStackTrace();
                    }                       
                    }               
                }
                
//******************************************************************************************************************************
//Aca va la parte que procesa el archivo rl_cambios_flota de oracle
//********************************************************************************************************************************

                String SQuery2="select * from sys.tables where name='rl_cambios_flotaTemp'" ;
                st1 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                rs1=st1.executeQuery(SQuery2);
                //System.out.println(pathSAP);
                rs1.last();
                if (rs1.getRow()>0){
                    System.out.println("TABLA rl_cambios_flotaTemp EXISTE"); 
                    st1 = conn.createStatement();
                    st1.executeUpdate("delete from rl_cambios_flotaTemp");
                    System.out.println("TABLA rl_cambios_flotaTemp BORRADA"); 
                     
                }else{
                    System.out.println("TABLA rl_cambios_flotaTemp NO EXISTE");                                         
                    st1=conn.createStatement();
// CBFLID,nvl(CBFLTPOFCODIGO,0), CBFLFLTAIDINTERNO, CBFLFECHA,CBFLFLTAORDENCO,CBFLFLTAOPERADORSAP                    
                    st1.executeUpdate("create table rl_cambios_flotaTemp(CBFLID numeric not null,CBFLTPOFCODIGO numeric  not null,CBFLFLTAIDINTERNO varchar(50) not null,CBFLFECHA datetime not null,CBFLFLTAORDENCO varchar(50) not null,CBFLFLTAOPERADORSAP varchar(50) not null)");
                    System.out.println("TABLA rl_cambios_flotaTemp CREADA"); 
                }
                    
                
                boolean ArchivoRl=false;
                
                String pathArchivoRl = new java.io.File(".").getCanonicalPath();
                String filesRl;
                FileReader frRl=null;                
                File folderRl = new File(pathArchivoRl);
                File[] listOfFilesRl = folderRl.listFiles(); 
            //Buscar arhivo que termina en ini.txt
                for (int i = 0; i < listOfFilesRl.length; i++) 
                {
                    if (listOfFilesRl[i].isFile()) 
                    {
                        filesRl = listOfFilesRl[i].getName();
                        if (filesRl.endsWith("rl_cambios_flota.txt"))
                            {
                                //System.out.println(files);
                                 ArchivoRl=true;
                                 break;
                            }else{
                            ArchivoRl=false;
                        }
                    }
                }
                if (ArchivoRl=true){                    
                    try {
                        String SQuery="";    
                        //String pathRl = new java.io.File(".").getCanonicalPath();
                        System.out.println("PROCESANDO ARCHIVO rl_cambios_flota.txt");       
                        Log.log("PROCESANDO ARCHIVO rl_cambios_flota.txt");
                        frRl = new FileReader (pathArchivoRl+"\\rl_cambios_flota.txt");
                        BufferedReader br = new BufferedReader(frRl);
                        String Registro="";
                        while((Linea=br.readLine())!=null){
                            //recuperar() ;
                            Registro=Linea;                    
                            //System.out.println(Registro);                     
                            SQuery="insert into rl_cambios_flotaTemp values ("+Registro+")";
                            //System.out.println(SQuery);
                            st1=conn.createStatement();
                            st1.executeUpdate(SQuery);                     
                        } 
                    } catch(Exception e){
                    e.printStackTrace();
                    }finally{
                    // En el finally cerramos el fichero, para asegurarnos
                    // que se cierra tanto si todo va bien como si salta 
                    // una excepcion.
                    try{                    
                    if( null != frRl ){   
                       frRl.close();     
                    }                  
                    }catch (Exception e2){ 
                    e2.printStackTrace();
                    }                       
                    }               
                }


//*********************************************************************************************************
//********************************************************************************************************

                String SQuery3="select * from sys.tables where name='MA_flotaTemp'" ;
                st1 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                rs1=st1.executeQuery(SQuery3);
                //System.out.println(pathSAP);
                rs1.last();
                if (rs1.getRow()>0){
                    System.out.println("TABLA MA_flotaTemp EXISTE"); 
                    st1 = conn.createStatement();
                    st1.executeUpdate("delete from MA_flotaTemp");
                    System.out.println("TABLA MA_flotaTemp BORRADA"); 
                     
                }else{
                    System.out.println("TABLA MA_flotaTemp NO EXISTE");                                         
                    st1=conn.createStatement();
//t TPOFCODIGO,nvl(FLTAIDINTERNO,0),nvl(FLTA_ORDEN_CO,'nulo'),nvl(FLTA_OPERADOR_SAP,'nulo') from OPRUTA.ma_flota
                    st1.executeUpdate("create table MA_flotaTemp(TPOFCODIGO numeric not null,FLTAIDINTERNO numeric  not null,FLTA_ORDEN_CO varchar(50) not null,FLTA_OPERADOR_SAP varchar(50) not null)");
                    System.out.println("TABLA MA_flotaTemp CREADA"); 
                }
                    
                
                boolean ArchivoMa=false;
                
                String pathArchivoMa = new java.io.File(".").getCanonicalPath();
                String filesMa;
                FileReader frMa=null;                
                File folderMa = new File(pathArchivoMa);
                File[] listOfFilesMa = folderMa.listFiles(); 
            //Buscar arhivo que termina en ini.txt
                for (int i = 0; i < listOfFilesMa.length; i++) 
                {
                    if (listOfFilesMa[i].isFile()) 
                    {
                        filesMa = listOfFilesMa[i].getName();
                        if (filesMa.endsWith("MA_flota.txt"))
                            {
                                //System.out.println(files);
                                ArchivoMa=true;
                                break;
                            }else{
                            ArchivoMa=false;
                        }
                    }
                }
                if (ArchivoMa=true){                    
                    try {
                        String SQuery="";    
                        //String pathRl = new java.io.File(".").getCanonicalPath();
                        System.out.println("PROCESANDO ARCHIVO MA_flota.txt");                    
                        frMa = new FileReader (pathArchivoMa+"\\MA_flota.txt");
                        BufferedReader br = new BufferedReader(frMa);
                        String Registro="";
                        while((Linea=br.readLine())!=null){
                            //recuperar() ;
                            Registro=Linea;                    
                            //System.out.println(Registro);                     
                            SQuery="insert into MA_flotaTemp values ("+Registro+")";
                            //System.out.println(SQuery);
                            st1=conn.createStatement();
                            st1.executeUpdate(SQuery);                     
                        } 
                    } catch(Exception e){
                    e.printStackTrace();
                    }finally{
                    // En el finally cerramos el fichero, para asegurarnos
                    // que se cierra tanto si todo va bien como si salta 
                    // una excepcion.
                    try{                    
                    if( null != frMa ){   
                       frMa.close();     
                    }                  
                    }catch (Exception e2){ 
                    e2.printStackTrace();
                    }                       
                    }               
                }
                
                   
//******************************************************************************************************************************
//Aca va la parte que crea el archivo final
//********************************************************************************************************************************
                System.out.println("CREANDO ARCHIVO");
                BuscarArchivoSAP(conn);
                

                //selectTrans(conn);
                Log.log("Se cierra conexion");
                conn.close();
                //Thread.sleep(60000);
                _Flag=true;
                
            }        
            catch (AccessControlException e)
            {
                System.out.println("AC Err");
                Log.log("AC Err: "+e.toString());
                e.printStackTrace();
            }
            catch (SQLException s)
            {
                System.out.println("SQ Err");
                Log.log("SQ Err: "+s.toString());
                s.printStackTrace();
                conn.close();
            }
        }
    }
    catch(Exception e)
    {
        System.out.println("OT Err");
        Log.log("OT Err: "+e.toString());
        e.printStackTrace();
    }
 }
public static void  BuscarINI() throws IOException{
    
    //System.out.println("BUSCA ARCHIVO INI");    
    String ruta = "Hecpoll_ini.txt";
    String path = new java.io.File(".").getCanonicalPath();
    String files;
    FileReader fr=null;                
    File folder = new File(path);
    File[] listOfFiles = folder.listFiles(); 
//Buscar arhivo que termina en ini.txt
    for (int i = 0; i < listOfFiles.length; i++) 
    {
        if (listOfFiles[i].isFile()) 
        {
            files = listOfFiles[i].getName();
            if (files.endsWith("Hecpoll_ini.txt"))
                {
                   // System.out.println("ARCHIVO Hecpoll_ini.txt YA EXISTE");
                    existe=true;
                }
        }
    }
 //Si no existe creamos archivo con parametros de fabrica en duro.       
    if(existe==false) {
           try
            {
            
                System.out.println("NO EXISTE ARCHIVO, CREAR...");
                Log.log("NO EXISTE ARCHIVO, CREAR...");
                PrintWriter escribir = new PrintWriter(new FileWriter("Hecpoll_ini.txt",true));
                escribir.println("{\"servername1\":\"172.20.4.157\",\"databaseName1\":\"SAP\",\"user1\":\"SAP\",\"pass1\":\"sap2016\",\"servername2\":\"172.20.4.178\",\"databaseName2\":\"gxsuitop\",\"user2\":\"gxuser\",\"pass2\":\"gx.2017\",\"port2\":\"1521\",\"servername3\":\"172.24.6.84\",\"databaseName3\":\"HECPOLL\",\"user3\":\"sa\",\"pass3\":\"FleetAccess1$\",\"rutaSap\":\"C:\\SAP\",\"sFTP\":\"172.20.7.176\",\"ftpUser\":\"transfer\",\"ftpPassword\":\"Tu48u5\",\"remotePath\":\"/INMM2/\",\"HoraS\":\"10\",\"MinutoS\":\"00\",\"IDPAYMEN\":\"0\",\"estado\":0}");    			
                escribir.close();
                BuscarINI();
            }
            catch(IOException e)
            {

            }
        } 
  //Si existe, leemos contenido.  
    else {   
        if(existe==true) {
            try {
               //bw = new BufferedWriter(new FileWriter(archivo));
               //System.out.println("EXISTE ARCHIVO");
               // bw.write("El fichero de texto ya estaba creado.");
               fr = new FileReader (ruta);
               BufferedReader br = new BufferedReader(fr);

            while((Linea=br.readLine())!=null)
                 recuperar() ;
               // System.out.println(linea);
            } catch(Exception e){
             e.printStackTrace();
            }finally{
         // En el finally cerramos el fichero, para asegurarnos
         // que se cierra tanto si todo va bien como si salta 
         // una excepcion.
            try{                    
               if( null != fr ){   
                  fr.close();     
               }                  
            }catch (Exception e2){ 
               e2.printStackTrace();
            }
           }
        }         
    }
}
    
      
public static void  BuscarArchivoSAP(Connection conn) throws IOException, Exception{
        
        System.out.println("BUSCANDO ARCHIVO SAP");        
        
        Calendar c = Calendar.getInstance();
        c.setTime(new Date(System.currentTimeMillis()));
        int mes = c.get(Calendar.MONTH)+1;
        int ano= c.get(Calendar.YEAR);
        int dia=c.get(Calendar.DAY_OF_MONTH);
        String sdia="",smes="";
        
        if (dia<10)
            sdia="0"+dia;
        else
            sdia=""+dia;
        if (mes<10)
            smes="0"+mes;
        else
            smes=""+mes;
        
        String Archivo = ano+""+smes+""+sdia+".txt";
        String files="";

                
        File folder = new File(rutaSap+"\\");
        
        if (!folder.isDirectory()) { 
        folder.mkdir();
        }
        
        File[] listOfFiles = folder.listFiles(); 
        System.out.println("RUTA SAP:"+rutaSap);
        System.out.println("BUSCANDO ARCHIVO:"+Archivo);
        for (int i = 0; i < listOfFiles.length; i++) 
        {
            if (listOfFiles[i].isFile()) 
            {
                files = listOfFiles[i].getName();
                if (files.endsWith(Archivo))
                {
                    if(!files.equals("")){
                    //System.out.println(files);
                    existeSap=true;
                    break;
                    }
                }
            }
        }
          
              
    if(existeSap==false) {
           try
            {
                //est=true;
                //System.out.println("JSON Buscar Sap: "+Linea2+"\n");
                modificarEstado(Linea2);
                System.out.println("INICIO SELECT");
                selectTrans(conn);
                
            }
            catch(IOException e)
            {

            }
        } 
    else 
        {   
    if(existeSap==true){

        try{
        System.out.println("YA EXISTE ARCHIVO SAP "+existeSap);           
           File fichero = new File(rutaSap+"\\"+Archivo);
          
        if (fichero.delete()){
             System.out.println("EL ARCHIVO SAP FUE BORRADO");
             existeSap=false;
             
           }
        else{
          System.out.println("EL ARCHIVO SAP NO PUDO SER BORRADO");}
          Log.log("BuscarArchivoSAP Ex Err: El fichero no puede ser borrado");
          BuscarArchivoSAP(conn);
        } 
        catch(Exception e){
         e.printStackTrace();
      }finally{
         // En el finally cerramos el fichero, para asegurarnos
         // que se cierra tanto si todo va bien como si salta 
         // una excepcion.
         try{                    
            //if( null != fr ){   
            //   fr.close();     
           // }       

           
         }catch (Exception e2){ 
            e2.printStackTrace();
             Log.log("BuscarArchivoSAP Ex Err: "+e2.toString());
         }
        }
    }         
 }
    
    
}
   
public static void modificarEstado(String linea) throws Exception 
{   
    System.out.println("MODIFICANDO Json");    
    System.out.println("JSON modificarEstado : "+linea+"");

    if (linea !=null)
    {
        //if(est==true){
             try
            {
                PrintWriter escribir = new PrintWriter(new FileWriter("Hecpoll_ini.txt",false));
                Linea2="{\"servername1\":\""+servername1+"\",\"databaseName1\":\""+databaseName1+"\",\"user1\":\""+user1+"\",\"pass1\":\""+pass1+"\",\"servername2\":\""+servername2+"\",\"databaseName2\":\""+databaseName2+"\",\"user2\":\""+user2+"\",\"pass2\":\""+pass2+"\",\"port2\":\""+port2+"\",\"servername3\":\""+servername3+"\",\"databaseName3\":\""+databaseName3+"\",\"user3\":\""+user3+"\",\"pass3\":\""+pass3+"\",\"rutaSap\":\""+rutaSap+"\",\"sFTP\":\""+sFTP+"\",\"ftpUser\":\""+ftpUser+"\",\"ftpPassword\":\""+ftpPassword+"\",\"remotePath\":\""+remotePath+"\",\"HoraS\":\""+HoraS+"\",\"MinutoS\":\""+MinutoS+"\",\"IDPAYMEN\":\""+IDPAYMEN+"\",\"estado\":"+estado+"}";
                escribir.println("{\"servername1\":\""+servername1+"\",\"databaseName1\":\""+databaseName1+"\",\"user1\":\""+user1+"\",\"pass1\":\""+pass1+"\",\"servername2\":\""+servername2+"\",\"databaseName2\":\""+databaseName2+"\",\"user2\":\""+user2+"\",\"pass2\":\""+pass2+"\",\"port2\":\""+port2+"\",\"servername3\":\""+servername3+"\",\"databaseName3\":\""+databaseName3+"\",\"user3\":\""+user3+"\",\"pass3\":\""+pass3+"\",\"rutaSap\":\""+rutaSap+"\",\"sFTP\":\""+sFTP+"\",\"ftpUser\":\""+ftpUser+"\",\"ftpPassword\":\""+ftpPassword+"\",\"remotePath\":\""+remotePath+"\",\"HoraS\":\""+HoraS+"\",\"MinutoS\":\""+MinutoS+"\",\"IDPAYMEN\":\""+IDPAYMEN+"\",\"estado\":"+estado+"}");    	
                escribir.close();
            }
            catch(IOException e)
            {
                 Log.log("modificarEstado IO Err: "+e.toString());
            }

    }
}  

 static Boolean EliminaTXT( String Archivo ) throws Exception 
{   
        boolean existe=false;
        //String Archivo = "rl_cambios_flota.txt";
        String files="";
        String path = new java.io.File(".").getCanonicalPath();

                
        File folder = new File(path+"\\");
        
        if (!folder.isDirectory()) { 
        folder.mkdir();
        }
        
        File[] listOfFiles = folder.listFiles(); 
        //System.out.println("RUTA TXT:"+folder);
        //System.out.println("BUSCANDO ARCHIVO:"+Archivo);
        for (int i = 0; i < listOfFiles.length; i++) 
        {
            if (listOfFiles[i].isFile()) 
            {
                files = listOfFiles[i].getName();
                    if (files.endsWith(Archivo))
                    {
                        if(!files.equals("")){
                        //System.out.println(files);
                        existe=true;
                        break;
                    }
                }
            }
        }
          
           System.out.println("Existe Archivo: "+existe);   
    if(existe==false) {
         
                System.out.println("ELIMINA TXT: NO EXISTE ARCHIVO A ELIMINAR");
                existe=false;
                
        } 
    else 
        {   
    if(existe==true){

        try{
        System.out.println("YA EXISTE ARCHIVO SAP "+existe);           
           File fichero = new File(path+"\\"+Archivo);
           System.out.println("FICHERO "+fichero);   
        if (fichero.delete()){
             System.out.println("ELIMINA TXT: EL ARCHIVO FUE BORRADO");
            existe=false;
           }
        else{
          System.out.println("ELIMINA TXT: EL ARCHIVO SAP NO PUDO SER BORRADO");}
        } 
        catch(Exception e){
        e.printStackTrace();
        Log.log("ELIMINA TXT Ex Err: "+e.toString());
         
      }finally{
         // En el finally cerramos el fichero, para asegurarnos
         // que se cierra tanto si todo va bien como si salta 
         // una excepcion.
         try{                    
            //if( null != fr ){   
            //   fr.close();     
           // }       

           
         }catch (Exception e2){ 
            e2.printStackTrace();
            Log.log("ELIMINA TXT: Ex Err: "+e2.toString());
         }
        }
        
    }         
 }
    return existe;
}  
    
  
   
    public static void recuperar() throws Exception 
{   
    System.out.println("RECUPERANDO DATOS DE .INI");
    
   // Log.log("Recuperar INI \n");
    //Linea2=Linea;
    //System.out.println("JSON recuperar: "+Linea);

    if (Linea !=null)
    {
        String Lineap=Linea.replace("\\", "\\\\");
        //System.out.println("JSON : "+Lineap.replace("\\\\","\\\\"));
        Linea2=Lineap;
        Linea=Lineap;
        
        String jsonText = Linea;
        JSONObject json = new JSONObject(jsonText);
        System.out.println("JSON:"+json.toString()+"");
        
        servername1=json.get("servername1").toString();
        databaseName1=json.get("databaseName1").toString(); 
        user1=json.get("user1").toString();
        pass1=json.get("pass1").toString();
        servername2=json.get("servername2").toString();
        databaseName2=json.get("databaseName2").toString(); 
        user2=json.get("user2").toString();
        pass2=json.get("pass2").toString();
        port2=json.get("port2").toString();
        servername3=json.get("servername3").toString();
        databaseName3=json.get("databaseName3").toString(); 
        user3=json.get("user3").toString();
        pass3=json.get("pass3").toString();
        rutaSap=json.get("rutaSap").toString();
        
        sFTP=json.get("sFTP").toString();
        ftpUser=json.get("ftpUser").toString();
        
        ftpPassword=json.get("ftpPassword").toString();
        remotePath=json.get("remotePath").toString();
        HoraS=json.get("HoraS").toString();
        MinutoS=json.get("MinutoS").toString();
        IDPAYMEN=json.get("IDPAYMEN").toString();
        IDPAYMENini=json.get("IDPAYMEN").toString();
        estado=json.get("estado").toString();
        /*
        
        System.out.println("Servername1 : "+servername1);
        System.out.println("DatabaseName1 : "+databaseName1);
        System.out.println("User1 : "+user1);
        System.out.println("Pass1 : "+pass1);
        System.out.println("Servername2 : "+servername2);
        System.out.println("DatabaseName2 : "+databaseName2);
        System.out.println("User2 : "+user2);
        System.out.println("Pass2 : "+pass2);
        System.out.println("Port2 : "+port2);
        System.out.println("Servername3 : "+servername3);
        System.out.println("DatabaseName3 : "+databaseName3);
        System.out.println("User3 : "+user3);
        System.out.println("Pass3 : "+pass3);
        System.out.println("Ruta Sap : "+rutaSap);
        System.out.println("HoraS : "+HoraS);
        System.out.println("MinutoS : "+MinutoS);
        System.out.println("IDPAYMEN : "+IDPAYMEN);
        System.out.println("estado : "+estado+"\n");*/
    }
}
    
    public static void selectTrans(Connection conn) throws Exception
    {
        AUXidPaymen=IDPAYMEN;
        
        System.out.println("RESCATE DE DATOS TRANSACCIONES");        
       
        st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);        


        String Sql1 ="select DISTINCT ID_Payments, case a.TransArticleDescription when 'AdBlue' then 'ADBLUE' else 'COMBUSTIBLE' end,\n" +
"c.SiSaSitioSAP, SUBSTRING(convert(varchar, a.TransDateTime ,112), 1, 10) as Fecha,\n" +
"replace(SUBSTRING(convert(varchar, a.TransDateTime ,120), 12, 21),':','') as Hora,\n" +
"isnull(a.CardNumber,SUBSTRING(a.CardPAN,1,6)) as CardNumber, e.Description, \n" +
"case when a.CardNumber IS NULL then case when LEN(a.CardPAN)>2 and LEN(a.CardPAN)<8 and \n" +
" 	 (ascii(SUBSTRING(a.CardPAN,1,1))between 48 and 57  and ascii(SUBSTRING(a.CardPAN,2,1))between 48 and 57  and ascii(SUBSTRING(a.CardPAN,3,1))between 48 and 57 \n" +
" 	 and ascii(SUBSTRING(a.CardPAN,4,1))between 48 and 57 and ascii(SUBSTRING(a.CardPAN,5,1))between 48 and 57  and ascii(SUBSTRING(a.CardPAN,6,1))between 48 and 57 )\n" +
" 	 then isnull((select top 1 rl.CBFLFLTAORDENCO from rl_cambios_flotatemp rl where rl.CBFLFLTAIDINTERNO = substring(a.CardPAN,3,4)and rl.CBFLFECHA>=a.TransDateTime),\n" +
"	 (select top 1 ma.FLTA_ORDEN_CO from MA_flotatemp ma where ma.FLTAIDINTERNO = convert(numeric,SUBSTRING(a.CardPAN,3,4))))        \n" +
"	 else null end else\n" +
"	 case when LEN(a.CardNumber)>2 then isnull((select top 1 rl.CBFLFLTAORDENCO from rl_cambios_flotatemp rl where rl.CBFLFLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID)and rl.CBFLFECHA>=a.TransDateTime),\n" +
"	(select top 1 ma.FLTA_ORDEN_CO from MA_flotatemp ma where ma.FLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID)))        \n" +
"	 else isnull((select top 1 rl.CBFLFLTAORDENCO from rl_cambios_flotatemp rl where rl.CBFLFLTAIDINTERNO =(SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID) and rl.CBFLFECHA>=a.TransDateTime),\n" +
"	(select top 1 ma.FLTA_ORDEN_CO from MA_flotatemp ma where ma.FLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID)))end \n" +
" end as NumeroMaquinaSAP ,a.TransQuantity ,a.Mileage, case when a.CardNumber IS NULL then \n" +
" 	 case when LEN(a.CardPAN)>2 and LEN(a.CardPAN)<8 and (ascii(SUBSTRING(a.CardPAN,1,1))between 48 and 57  and ascii(SUBSTRING(a.CardPAN,2,1))between 48 and 57  and ascii(SUBSTRING(a.CardPAN,3,1))between 48 and 57 \n" +
" 	 and ascii(SUBSTRING(a.CardPAN,4,1))between 48 and 57 and ascii(SUBSTRING(a.CardPAN,5,1))between 48 and 57  and ascii(SUBSTRING(a.CardPAN,6,1))between 48 and 57 ) \n" +
" 	 then case when isnull((select top 1 rl.CBFLTPOFCODIGO from rl_cambios_flotatemp rl where  rl.CBFLFLTAIDINTERNO = SUBSTRING(a.CardPAN,3,4) and rl.CBFLFECHA>=a.TransDateTime),(select top 1 ma.TPOFCODIGO from MA_flotatemp ma where  ma.FLTAIDINTERNO = SUBSTRING(a.CardPAN,3,4)))=0 then 'B' \n" +
"		else case when isnull((select top 1 rl.CBFLTPOFCODIGO from rl_cambios_flotatemp rl where  rl.CBFLFLTAIDINTERNO = SUBSTRING(a.CardPAN,3,4)and rl.CBFLFECHA>=a.TransDateTime),(select top 1 ma.TPOFCODIGO from MA_flotatemp ma where ma.FLTAIDINTERNO = SUBSTRING(a.CardPAN,3,4)))=1 then 'C'\n" +
"		else 'V' end end else null 	end	else\n" +
"	case when LEN(a.CardNumber)>2 then \n" +
"		case when isnull((select top 1 rl.CBFLTPOFCODIGO from rl_cambios_flotatemp rl where  rl.CBFLFLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID) and rl.CBFLFECHA>=a.TransDateTime),(select top 1 ma.TPOFCODIGO from MA_flotatemp ma where  ma.FLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID)))=0 then 'B' \n" +
"		else \n" +
"			case when isnull((select top 1 rl.CBFLTPOFCODIGO from rl_cambios_flotatemp rl where  rl.CBFLFLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID)and rl.CBFLFECHA>=a.TransDateTime),(select top 1 ma.TPOFCODIGO from MA_flotatemp ma where ma.FLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID)))=1 then 'C'\n" +
"			else 'V' 	end 		end 	else \n" +
"		case when isnull((select top 1 rl.CBFLTPOFCODIGO from rl_cambios_flotatemp rl where  rl.CBFLFLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID) and rl.CBFLFECHA>=a.TransDateTime),(select top 1 ma.TPOFCODIGO from MA_flotatemp ma where  ma.FLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID)))=0 then 'B' \n" +
"		else 	case when isnull((select top 1 rl.CBFLTPOFCODIGO from rl_cambios_flotatemp rl where  rl.CBFLFLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID) and rl.CBFLFECHA>=a.TransDateTime),(select top 1 ma.TPOFCODIGO from MA_flotatemp ma where ma.FLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID)))=1 then 'C'\n" +
"			else 'V' end end end end	as TipoFlota, case when a.CardNumber IS NULL then\n" +
"	case when LEN(a.CardPAN)>2 and LEN(a.CardPAN)<8 and (ascii(SUBSTRING(a.CardPAN,1,1))between 48 and 57  and ascii(SUBSTRING(a.CardPAN,2,1))between 48 and 57  and ascii(SUBSTRING(a.CardPAN,3,1))between 48 and 57 \n" +
" 		and ascii(SUBSTRING(a.CardPAN,4,1))between 48 and 57 and ascii(SUBSTRING(a.CardPAN,5,1))between 48 and 57  and ascii(SUBSTRING(a.CardPAN,6,1))between 48 and 57 ) \n" +
" 	then isnull(Substring( convert(varchar,(select top 1 rl.CBFLFLTAOPERADORSAP from rl_cambios_flotatemp rl where  rl.CBFLFLTAIDINTERNO = SUBSTRING(a.CardPAN,3,4) and rl.CBFLFECHA>=a.TransDateTime)),1,4), \n" +
"		Substring( convert(varchar,(select top 1 ma.FLTA_OPERADOR_SAP from MA_flotatemp ma where  ma.FLTAIDINTERNO =SUBSTRING(a.CardPAN,3,4))),1,4)) \n" +
"	else null	end  else	case when len(a.CardNumber)>2 then  \n" +
"	isnull(Substring( convert(varchar,(select top 1 rl.CBFLFLTAOPERADORSAP from rl_cambios_flotatemp rl where  rl.CBFLFLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID) and rl.CBFLFECHA>=a.TransDateTime)),1,4), \n" +
"	Substring( convert(varchar,(select top 1 ma.FLTA_OPERADOR_SAP from MA_flotatemp ma where  ma.FLTAIDINTERNO =(SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID))),1,4)) \n" +
"	else 	isnull(Substring(convert(varchar,(select top 1 rl.CBFLFLTAOPERADORSAP from rl_cambios_flotatemp rl where rl.CBFLFLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID)and rl.CBFLFECHA>=a.TransDateTime)),1,4),\n" +
"	Substring( convert(varchar,(select top 1 ma.FLTA_OPERADOR_SAP from MA_flotatemp ma where  ma.FLTAIDINTERNO = (SELECT Number from VEHICLES where ID_VEHICLES=a.VehiclesID))),1,4))\n" +
"	end end as OperadorSAP,  "+
"case (SUBSTRING(a.cardpan,1,2)) when '10' then SUBSTRING(a.cardpan,3,4) else g.Number end  , h.DeviceAddress from  PAYMENTS a inner join terminals b on a.terminalsid=b.ID_Terminals \n" +
"left join MA_SitioSAPTemp c on b.StationsID=c.SiSaSitioCombustible left join CARDS d on a.CardsID=d.ID_CARDS\n" +
"left join VEHICLES g on a.VehiclesID=g.ID_VEHICLES left join VEHICLEGROUPS f on g.VehicleGroupsID=f.ID_VEHICLEGROUPS\n" +
"left join CARDSYSTEMTYPES e on e.ID_CARDSYSTEMTYPES=isnull(d.cardsystemtypesid,(select top 1 cardsystemtypesid  from Cards where ID_Cards=CardsID2))\n" +
"left join TRANSACTIONS h on a.ID_PAYMENTS=h.ID_TRANSACTIONS\n" +

"where a.ID_payments>"+AUXidPaymen+" order by ID_PAYMENTS";
        System.out.println(Sql1);          
        rs = st.executeQuery(Sql1);
                  
    rs.next();
    if (!(rs.getRow()>0)){
    System.out.println("No existen datos para este dia"); 
    //String trama="No existen datos para este dia";
    //docHecpoll.docHecpoll(rutaSap, trama);
     Log.log("NO EXISTEN DATOS NUEVOS PARA ESTE DIA");
     estado="0";
     modificarEstado(Linea2);
    }
    else{
    while (rs.getRow()>0)
    {
    for(int i = 1; i < 15; i++){
            
        switch (i) 
                {
                    case 1:{
                    if(String.valueOf(rs.getObject(1)).equals("null") || String.valueOf(rs.getObject(1)).equals("NULL"))
                    {idPaymen ="NULL";}
                    else
                    {idPaymen=rs.getObject(1).toString();
                    AUXidPaymen=rs.getObject(1).toString();}
                    } break;
                    
                    case 2:{
                    if(String.valueOf(rs.getObject(2)).equals("null")||String.valueOf(rs.getObject(2)).equals("NULL"))
                    {combustible ="NULL";}
                    else
                    {combustible=rs.getObject(2).toString();}
                    }break;
                    
                    case 3:{
                    if(String.valueOf(rs.getObject(3)).equals("null")||String.valueOf(rs.getObject(3)).equals("NULL"))
                    {sitio ="NULL";}
                    else
                    {sitio=rs.getObject(3).toString();}
                    }break;
                    
                    case 4:{
                    if(String.valueOf(rs.getObject(4)).equals("null")||String.valueOf(rs.getObject(4)).equals("NULL"))
                    {fecha ="NULL";}
                    else
                    {fecha=rs.getObject(4).toString();}
                    }break;
                    
                    case 5:{
                    if(String.valueOf(rs.getObject(5)).equals("null")||String.valueOf(rs.getObject(5)).equals("NULL"))
                    {hora ="NULL";}
                    else
                    {hora=rs.getObject(5).toString();}
                    }break;
                    
                    case 6:{
                    if(String.valueOf(rs.getObject(6)).equals("null")||String.valueOf(rs.getObject(6)).equals("NULL"))
                    {trx ="NULL";}
                    else
                    {trx=rs.getObject(6).toString();}
                     }break;
                                        
                    case 7:{
                   
                    if(String.valueOf(rs.getObject(7)).equals("null")||String.valueOf(rs.getObject(7)).equals("NULL"))
                    {tipoTRX ="NULL";}
                    else
                    {tipoTRX=rs.getObject(7).toString();}
                     } break;
                     
                     case 8:{
                   
                    if(String.valueOf(rs.getObject(8)).equals("null")||String.valueOf(rs.getObject(8)).equals("NULL"))
                    {maquina ="NULL";}
                    else
                    {maquina=rs.getObject(8).toString();}
                     } break;
                     
                    case 9:{
                    if(String.valueOf(rs.getObject(9)).equals("null")||String.valueOf(rs.getObject(9)).equals("NULL"))
                    {litros ="NULL";}
                    else
                    {litros=rs.getObject(9).toString();}
                    }break;
                    
                    case 10:{
                    if(String.valueOf(rs.getObject(10)).equals("null")||String.valueOf(rs.getObject(10)).equals("NULL"))
                    {odometro ="NULL";}
                    else
                    {odometro=rs.getObject(10).toString();}
                    } break;
                    
                     case 11:{
                    if(String.valueOf(rs.getObject(11)).equals("null")||String.valueOf(rs.getObject(11)).equals("NULL"))
                    {tipo ="NULL";}
                    else
                        {tipo=rs.getObject(11).toString();}
                    } break;
                    
                     case 12:{
                    if(String.valueOf(rs.getObject(12)).equals("null")||String.valueOf(rs.getObject(12)).equals("NULL"))
                    {operador ="NULL";}
                    else
                    {operador=rs.getObject(12).toString();}
                    } break;
                    
                     case 13:{
                    if(String.valueOf(rs.getObject(13)).equals("null")||String.valueOf(rs.getObject(13)).equals("NULL"))
                    {numero ="NULL";}
                    else
                    {numero=rs.getObject(13).toString();}
                    } break;
                    
                     case 14:{
                    if(String.valueOf(rs.getObject(14)).equals("null")||String.valueOf(rs.getObject(14)).equals("NULL"))
                    {pistola ="NULL";}
                    else
                    {pistola=rs.getObject(14).toString();}
                    } break;
                    
               }
            //System.out.println(idPaymen+","+sitio+","+fecha+","+hora+","+trx+","+tipoTRX+","+litros+","+odometro);
}
     
            String trama=""+combustible+";"+sitio+";"+fecha+";"+hora+";"+trx+";"+tipoTRX.substring(0, Math.min(10, tipoTRX.length()))+";"+maquina.replace(" ", "")+";"+litros+";"+odometro+";"+tipo.substring(0, Math.min(10, tipo.length()))+";"+operador+";"+numero+";"+pistola+";"+idPaymen;
            docHecpoll.docHecpoll(rutaSap, trama);
            //System.out.println(idPaymen+","+sitio+","+fecha+","+hora+","+trx+","+tipoTRX+","+litros+","+odometro);
            System.out.println(trama);
            AUXidPaymen=idPaymen;
             System.out.println(AUXidPaymen);
            rs.next();  
        }
        IDPAYMEN=AUXidPaymen;
        //est=false;
        enviaArchivo();
        estado="0";
        modificarEstado(Linea2);
         Log.log("Fin lectura");
         Log.log("IDPaiment inicial: " +IDPAYMENini+" Ultimo IDpaiment ingresado: "+IDPAYMEN);
         
        rs.close();
    }   
      
  }  
    
public static void enviaArchivo() throws SocketException, UnknownHostException, IOException, Exception {  
    System.out.println("SUBIR ARCHIVO AL FTP");
    Log.log("SUBIR ARCHIVO AL FTP");

     File f;
    //Linea2=Linea;
    //System.out.println("JSON recuperar: "+Linea);    
    
   System.out.println("BUSCANDO ARCHIVO HEC");        
        
        Calendar c = Calendar.getInstance();
        c.setTime(new Date(System.currentTimeMillis()));
        int mes = c.get(Calendar.MONTH)+1;
        int ano= c.get(Calendar.YEAR);
        int dia=c.get(Calendar.DAY_OF_MONTH);
        String smes="",sdia="";
               
        if (mes<10)
            smes="0"+mes;
        else
            smes=""+mes;
        if (dia<10)
            sdia="0"+dia;
        else
            sdia=""+dia;
            
        
        
        String Archivo = ano+""+smes+""+sdia+".txt";
        String files="";

                
        File folder = new File(rutaSap+"\\");
        
        if (!folder.isDirectory()) { 
        folder.mkdir();
        }
        
        File[] listOfFiles = folder.listFiles(); 
        System.out.println("RUTA SAP:"+rutaSap);
        System.out.println("BUSCANDO ARCHIVO:"+Archivo);
        for (int i = 0; i < listOfFiles.length; i++) 
        {
            if (listOfFiles[i].isFile()) 
            {
                files = listOfFiles[i].getName();
                if (files.endsWith(Archivo))
                {
                    if(!files.equals("")){
                    System.out.println("Archivos encontrados: "+files);
                    existeSap1=true;
                    break;
                    }
                }
            }
        }
          
             
    if(existeSap1==false) {
        
                System.out.println("NO SE A ENCONTRADO ARCHIVO HEC");  
                Log.log("NO SE A ENCONTRADO ARCHIVO HEC");
              
        } 
    else 
        { 
    System.out.println("existeSap1 :"+existeSap1);
    if(existeSap1==true){
  
        String pass = ftpPassword;//url
        String localPath = rutaSap+"/"+Archivo;
        //String remotePath = "/INMM2/Micrologica/"+Archivo;
        
        String user = ftpUser;
        String server = sFTP;//url
        
        InputStream inp = null;
        try {
            URL url = new URL("ftp://" + user + ":" + pass + "@" + server + remotePath+Archivo + ";type=i");
           // URL url = new URL("ftp://" + ftpUser + ":" + ftpPassword + "@" + sFTP + remotePath + ";type=i");
            URLConnection urlc = url.openConnection();
            OutputStream os = urlc.getOutputStream();
            File fichero = new File(localPath);
            inp = new FileInputStream(fichero);
            byte bytes[] = new byte[1024];
            int readCount = 0;

            while ((readCount = inp.read(bytes)) > 0) {
                os.write(bytes, 0, readCount);
            }
            os.flush();
            os.close();
            inp.close();


        } catch (Exception ex) {
            ex.printStackTrace();

        }

    
        
        
    }         
 }  

    }
    
    
}
