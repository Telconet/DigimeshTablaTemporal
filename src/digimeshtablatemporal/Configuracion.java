/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package digimeshtablatemporal;

import java.io.*;
import java.util.*;

/**
 *
 * @author Eduardo Murillo
 */
public class Configuracion {
    
    //Strings que buscaremos en el archivo de configuracion
    public static final String IP_BASE_DE_DATOS = "ip-base-datos";
    public static final String USUARIO_BASE_DE_DATOS = "usuario-base-datos";
    public static final String CLAVE_BASE_DE_DATOS = "clave-base-datos";
    public static final String NOMBRE_BASE_DATOS = "nombre-base-de-datos";
    public static final String PUERTO_BD = "puerto-BD";
    public static final String ORACLE_SID = "oracle-sid";
    public static final String SERVIDOR_NTP = "servidor-ntp";
    public static final String NOMBRE_TABLA_MEDICIONES = "nombre-tabla-mediciones";
    public static final String NOMBRE_TABLA_MEDICIONES_ACTUALES = "nombre-tabla-mediciones-actuales";
    public static final String RUTA_LISTA_WM = "ruta-lista-wm";
    
    //Propiedades donde almacenaremos los datos leidos del
    //archivo de texto
    private Properties propiedades;
    
    public Configuracion(String ruta){
        
        try{
            this.propiedades = new Properties();
            this.propiedades.load(new FileInputStream(ruta));
            
        }
        catch(Exception e){
            e.printStackTrace();
            System.out.println("Error al crear configuracion");
        }
    }
    
    /**
     * obtiene un parametro del archivo de base de datos
     * @param nombreParametro
     * @return
     */
    public String obtenerParametro(String nombreParametro){
        try{
            return this.propiedades.getProperty(nombreParametro); 
        }
        catch(Exception e){
            e.printStackTrace();
            System.out.println("Parametro no existe");
            return null;
        }
    }

    
}
