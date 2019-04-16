/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hecpoll;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author pcontador
 */
public class docHecpoll {
    public static void docHecpoll(String ruta,String mensaje)
    {
        Calendar c = Calendar.getInstance();
        c.setTime(new Date(System.currentTimeMillis()));
        int mes = c.get(Calendar.MONTH)+1;
        FileOutputStream fo;
    	try
    	{
            int dia;
            String smes="",sdia="";
            dia=c.get(Calendar.DAY_OF_MONTH);
            //System.out.println("Dia :"+dia);
            if (mes<10)
                smes="0"+mes;
            else
                smes=""+mes;
            if (dia<10)
            {
                sdia="0"+dia;
            }
            else{
                sdia=""+dia;
            }
                
            
            PrintWriter escribir = new PrintWriter(new FileWriter(ruta+"\\"+c.get(Calendar.YEAR)+smes+sdia +".txt",true));
            escribir.println(mensaje);    			
            escribir.close();
    	}
    	catch(IOException e)
    	{
    	  Log.log("IO Err: "+e.toString()+"\n");	
    	}
    }
    
}
