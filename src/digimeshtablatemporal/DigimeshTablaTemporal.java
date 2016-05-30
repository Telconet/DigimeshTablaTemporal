    /*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package digimeshtablatemporal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.sql.Timestamp;
/**
 *
 * @author Eduardo
 */
public class DigimeshTablaTemporal {

    /**
     * @param args the command line arguments
     */
    
     public static final int MYSQL_DB = 1;
     public static final int ORACLE_DB = 2;
     public static final int POSTGRE_DB = 3;
        
        
    public static void main(String[] args) {
        // TODO code application logic here
        //Necesitamos datos de la base de datos.
       
        try{
            Calendar calendario = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

             String horaInicio = sdf.format(calendario.getTime());
             //System.out.println(horaInicio);

             //System.out.println(args.length);
            //java -jar ejecutable  <tipo BD> <tiempo atras> <ruta archivo configuracion>  <aws | noaws>
            if(args.length < 4){
                System.out.println("Numero insuficiente de argumentos");
                System.exit(-1);
            }
            
            boolean usarAWS = false;
            System.out.println(args[3]);
            if(args[3].contains("noaws")){
                usarAWS = false;
            }
            else if(args[3].contains("aws")){
                usarAWS = true;
            }
            else usarAWS = false;
            
            if(usarAWS){
                System.out.println("Usando AWS");
            }


            String ruta = args[2];
            Configuracion conf = new Configuracion(ruta);


            int tiempoAtras = 0;
            try{
                tiempoAtras = Integer.parseInt(args[1]);        //De cuanto tiempo atras necesitamos la medicion

            }
            catch(NumberFormatException e){
                tiempoAtras = 5;
            }

            //Tipo de base de datos
            int tipoBD = -1;
            if(!(args[0].equals("mysql") || args[0].equals("postgres") || args[0].equals("oracle") )){
                System.out.println("Nombre de base de datos inválido. El programa se cerrará.");
                System.exit(-1);
            }
            else if(args[0].equals("mysql")){
                tipoBD = MYSQL_DB;
            }
            else if(args[0].equals("oracle")){
                tipoBD = ORACLE_DB;
            }
            else if(args[0].equals("postgres")){
                tipoBD = POSTGRE_DB;
            }


            String IPServidor = conf.obtenerParametro(Configuracion.IP_BASE_DE_DATOS);
            String baseDeDatos = conf.obtenerParametro(Configuracion.NOMBRE_BASE_DATOS);
            String puertoDB = conf.obtenerParametro(Configuracion.PUERTO_BD);
            String sid = conf.obtenerParametro(Configuracion.ORACLE_SID);

            //Nos conectamos a la base de datos
             StringBuilder url = null;
             StringBuilder urlAWS = null;
            switch(tipoBD){
                case MYSQL_DB:
                    url = new StringBuilder("jdbc:mysql://");
                    Class.forName("com.mysql.jdbc.Driver").newInstance();
                    url.append(IPServidor);
                    url.append(":");
                    url.append(puertoDB);
                    url.append("/");
                    url.append(baseDeDatos);
                    
                    //Amazon Web Services DB
                    if(usarAWS){
                        urlAWS = new StringBuilder("jdbc:mysql://");
                        Class.forName("com.mysql.jdbc.Driver").newInstance();
                        urlAWS.append(conf.obtenerParametro(Configuracion.IP_AWS));
                        urlAWS.append(":");
                        urlAWS.append(puertoDB);
                        urlAWS.append("/");
                        urlAWS.append(baseDeDatos);
                    }
                    
                    break;
                case ORACLE_DB:
                    url = new StringBuilder("jdbc:oracle:thin:@");
                    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
                    url.append(IPServidor);
                    url.append(":");
                    url.append(puertoDB);
                    url.append(":");
                    url.append(sid);
                    break;
                case POSTGRE_DB:
                    url = new StringBuilder("jdbc:postgresql://");
                    Class.forName("org.postgresql.Driver").newInstance();
                    url.append(IPServidor);
                    url.append("/");
                    url.append(baseDeDatos);
                    break;
                default:
                    //TODO escribir a log de errores.
                    System.out.println("No nos pudimos conectar a la base de datos");
                    System.exit(-1);
            }
            
            //Creamos la conexion
            Connection conexion = DriverManager.getConnection(url.toString(), conf.obtenerParametro(Configuracion.USUARIO_BASE_DE_DATOS), conf.obtenerParametro(Configuracion.CLAVE_BASE_DE_DATOS));
            
            //Conexion Amazon AWS
            Connection conexionAWS = null;
            if(usarAWS){
                 conexionAWS= DriverManager.getConnection(urlAWS.toString(), conf.obtenerParametro(Configuracion.USUARIO_BASE_DE_DATOS), conf.obtenerParametro(Configuracion.CLAVE_BASE_DE_DATOS));
            }
            
            //
            if(conexion == null){
                System.out.println("No nos pudimos conectar a la base de datos");
                System.exit(-1);
            }
            
            String consultaCreacion = "CREATE TABLE IF NOT EXISTS `" + conf.obtenerParametro(Configuracion.NOMBRE_TABLA_MEDICIONES_ACTUALES) + "` ("+
                                      "`id_wasp` text character set utf8 collate utf8_unicode_ci," +
                                      "`id_secret` text character set utf8 collate utf8_unicode_ci," +
                                      "`frame_type` int(11) default NULL," +
                                      "`frame_number` int(11) default NULL," +
                                      "`sensor` text character set utf8 collate utf8_unicode_ci," +
                                      "`value` text character set utf8 collate utf8_unicode_ci," +
                                      "`timestamp` timestamp NOT NULL default CURRENT_TIMESTAMP," +
                                      "`raw` text character set utf8 collate utf8_unicode_ci," +
                                      "`parser_type` tinyint(3) NOT NULL default '0'" +
                                      ") ENGINE=MyISAM  DEFAULT CHARSET=latin1" ;
            
            //System.out.println(consultaCreacion);
            Statement creacionStatement = conexion.createStatement();
            int status = creacionStatement.executeUpdate(consultaCreacion);
            
            //AWS
            if(usarAWS){
                Statement creacionStatementAWS = conexionAWS.createStatement();
                status = creacionStatementAWS.executeUpdate(consultaCreacion);
                 creacionStatementAWS.close(); 
            }
            
            
            
            
            
           // System.out.println(status);
            creacionStatement.close();  
            
            
            //Consultamos el registro más actual
            String consultaFecha = "SELECT timestamp FROM " + conf.obtenerParametro(Configuracion.NOMBRE_TABLA_MEDICIONES) + " order by timestamp desc limit 0,1";
            Statement consultaFechaStatement = conexion.createStatement();
            
            ResultSet setFecha;
            setFecha = consultaFechaStatement.executeQuery(consultaFecha);
            
            Timestamp fechaMasActual = null;
            while(setFecha.next()){
                fechaMasActual = setFecha.getTimestamp("timestamp");
            }
            
            setFecha.close();
            consultaFechaStatement.close();
            
            
            
            //Obtenemos las fechas
            Calendar cal = Calendar.getInstance(); 
            SimpleDateFormat antes = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat ahora = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            
            System.out.println(fechaMasActual.toString());
            
            
            
            cal.setTimeInMillis(fechaMasActual.getTime());
            
            String ahoraStr = antes.format(cal.getTime());      //Fecha/hora actual
            System.out.println(ahoraStr);
            
            //antes
            cal.add(Calendar.MINUTE, -1*tiempoAtras);
            String antesStr = antes.format(cal.getTime());
            System.out.println(antesStr);
                

            //Obtenemos las ultimas mediciones
            String consultaSeleccion = "SELECT id_wasp, id_secret, frame_type, frame_number, sensor, value, timestamp, raw, parser_type FROM " +  
                                        conf.obtenerParametro(Configuracion.NOMBRE_TABLA_MEDICIONES) + " where timestamp between \'"  + antesStr + "\' and \'" + ahoraStr + 
                                        "\' order by timestamp desc";
            
            System.out.println(consultaSeleccion);
            Statement seleccionStatement = conexion.createStatement();
            
            ResultSet set;
            set = seleccionStatement.executeQuery(consultaSeleccion);
            
            System.out.println(consultaSeleccion + "\n\n");
            
            
            
            //Aqui tenemos ya  las medicions más actuales...Esta fallando aquí
            while(set.next()){
                
                //drop all
                String id_waspmote = set.getString("id_wasp");
                String sensor = set.getString("sensor");
                
                String consultaUpdate = "UPDATE " + conf.obtenerParametro(Configuracion.NOMBRE_TABLA_MEDICIONES_ACTUALES) + " SET id_secret = "  + set.getString("id_secret") + 
                                            ", frame_type = " + set.getString("frame_type") + ", frame_number = " + set.getString("frame_number") + ", sensor = '" + set.getString("sensor") +
                                            "', value = " + set.getString("value") + ", timestamp = '" + set.getString("timestamp") + "',  raw = '0'"  +
                                            ", parser_type = " + set.getString("parser_type") + " WHERE id_wasp = '" + set.getString("id_wasp") + "' AND sensor = '" + set.getString("sensor") + "'";
                
                System.out.println(consultaUpdate + "\n\n");
                
                //System.out.println(consultaUpdate);
                Statement updateStatement = conexion.createStatement();
                
                //Para servidor local
                int filasAfectadas = updateStatement.executeUpdate(consultaUpdate);

                //Si no se afectaron filas, es por que no exisitia el waspmote con el sensor, entonces lo isnertamos.
                if(filasAfectadas == 0){ 
                    //TODO: insertar por que no existe...
                    String consultaInsercion = "INSERT INTO " + conf.obtenerParametro(Configuracion.NOMBRE_TABLA_MEDICIONES_ACTUALES) + " (id_wasp, id_secret, frame_type, frame_number, sensor, value, timestamp, raw, parser_type)" + 
                                               " VALUES ('"  + set.getString("id_wasp") + "', '" + set.getString("id_secret") + "',  " +  set.getString("frame_type") + ", " + set.getString("frame_number") + 
                                               ", '" + set.getString("sensor") + "', '" + set.getString("value") + "', '" + set.getString("timestamp") + "', '0', "  +
                                               set.getString("parser_type") + " )";
                    
                    //System.out.println(consultaInsercion);
                    
                    Statement statementInsercion = conexion.createStatement();
                    int filas = statementInsercion.executeUpdate(consultaInsercion);
                    
                    if(filas == 0){
                        System.out.println("No se pudo insertar el registro.");
                    }
                    statementInsercion.close();
                }
                
                updateStatement.close();
                
                if(usarAWS){
                    //Para servidor AWS
                    Statement updateStatementAWS = conexionAWS.createStatement();
                    filasAfectadas = updateStatementAWS.executeUpdate(consultaUpdate);
                

                    //Si no se afectaron filas, es por que no exisitia el waspmote con el sensor, entonces lo isnertamos.
                    if(filasAfectadas == 0){ 
                        //TODO: insertar por que no existe...
                        String consultaInsercion = "INSERT INTO " + conf.obtenerParametro(Configuracion.NOMBRE_TABLA_MEDICIONES_ACTUALES) + " (id_wasp, id_secret, frame_type, frame_number, sensor, value, timestamp, raw, parser_type)" + 
                                                   " VALUES ('"  + set.getString("id_wasp") + "', '" + set.getString("id_secret") + "',  " +  set.getString("frame_type") + ", " + set.getString("frame_number") + 
                                                   ", '" + set.getString("sensor") + "', '" + set.getString("value") + "', '" + set.getString("timestamp") + "', '0', "  +
                                                   set.getString("parser_type") + " )";

                        //System.out.println(consultaInsercion);

                        Statement statementInsercionAWS = conexionAWS.createStatement();
                        int filas = statementInsercionAWS.executeUpdate(consultaInsercion);

                        if(filas == 0){
                            System.out.println("No se pudo insertar el registro en el servidor AWS.");
                        }
                        statementInsercionAWS.close();
                    }

                    //Cerramos los statements...
                    
                    updateStatementAWS.close();
                }
                
            }
            set.close();
            seleccionStatement.close();  
            conexion.close();                   //Cerramos la conexion
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
 