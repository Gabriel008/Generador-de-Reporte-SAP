/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hecpoll;

import java.io.*;
import java.util.Date;
import java.util.Calendar;

/**
 *
 * @author Gabriel
 */
public class docSap {
public static void docSap(String ruta,String mensaje)
    {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date(System.currentTimeMillis()));
        int mes = c.get(Calendar.MONTH)+1;
        FileOutputStream fo;
    	try
    	{
            PrintWriter escribir = new PrintWriter(new FileWriter(ruta+"\\"+"MA_SitioSAP.txt",true));
            escribir.println(mensaje);    			
            escribir.close();
    	}
    	catch(IOException e)
    	{
    	  Log.log("IO Err: "+e.toString()+"\n");	
    	}
    }
    
}
