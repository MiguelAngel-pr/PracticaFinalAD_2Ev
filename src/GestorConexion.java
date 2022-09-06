import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author migue
 */
public class GestorConexion 
{
    Connection conn1 = null; //Variable que almacena la conexión de la BBDD
    String bbdd = "centroestudios";//String que almacena la base de datos que usaremos, si se cambia se usará otra
    public GestorConexion()
    {
        abrir_conexion();
    }
    public int abrir_conexion()//Método usado para conectarse a la BBDD
    {
        try
        {
            String url = "jdbc:mysql://localhost:3306/" + bbdd + "?serverTimezone=UTC";
            String user = "root";
            String password = "";
            conn1 = DriverManager.getConnection(url, user, password);
            if(conn1 != null)
            {
                System.out.println("Te has conectado correctamente");
                return 0;
            }
            else
            {
                System.out.println("Ha habido un error al conectarte");
                return -1;
            }
        }
        catch(SQLException ex)
        {
            return -1;
        }
    }
    
    public String[] obtenerTitulos(String tabla, String tipo)//Método con el que se obtiene una lista con todos los campos o las tablas
    {
        Statement sta;
        String nombre_tabla[] = null; 
        String query = "";
        try 
        {
            sta = conn1.createStatement();
            if(tipo.equals("tablas")) //Dependiendo de si es una tabla o un campo se realizará una consulta u otra.
            {
                nombre_tabla = new String [obtenerNTablas()]; 
                query = "SELECT table_name AS nombre FROM information_schema.tables WHERE table_schema = '" + bbdd + "';";
            }
            else if(tipo.equals("campos"))
            {
                nombre_tabla = new String [obtenerNCampos(tabla)]; 
                query = "SELECT column_name as nombre FROM information_schema.columns WHERE table_name = '" + tabla + "'";
            }
            ResultSet rs = sta.executeQuery(query);
            int i=0;
            while (rs.next())
            {
                nombre_tabla[i]=rs.getString("nombre");
                i++;
            }
            rs.close();
            sta.close();
        } 
        catch (SQLException ex) 
        {
            System.out.println("Error");
        }
        return nombre_tabla;
    }
    
    public int obtenerNCampos(String tabla)//Método con el que se obtiene el nº de campos de una tabla
    {
        Statement sta;
        int nColumnas = 0;
        try 
        {
            sta = conn1.createStatement();
            String query = "Select * from " + tabla + "";
            ResultSet rs = sta.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            nColumnas = rsmd.getColumnCount();
            rs.close();
            sta.close();
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return nColumnas;
    }
    
    public int obtenerNTablas()//Método con el que se obtiene el nº de tablas de una base de datos
    {
        Statement sta;
        int nTablas = 0;
        try 
        {
            sta = conn1.createStatement();
            String query = "SELECT COUNT(*) as count from Information_Schema.Tables WHERE table_schema = '" + bbdd + "';";
            ResultSet rs = sta.executeQuery(query);
            if(rs.next())
            {
                nTablas = rs.getInt("count");
            }
            rs.close();
            sta.close();
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return nTablas;
    }
    
    public int insertarTabla(String nombre, String campo, String tipo_campo, String longitud)//Método con el que se inserta una tabla en la base de datos
    {
        Statement sta;
        try 
        {
            sta = conn1.createStatement();
            sta.executeUpdate("CREATE TABLE " + bbdd + "." + nombre + " (`" + campo + "` " + tipo_campo + "(" + longitud + ") NOT NULL PRIMARY KEY);");
            sta.close();
            return 0;
        } 
        catch (SQLException ex) 
        {
            return -1;
        }
    }
    public int insertaDatos(String tabla, String campos[], String[] valores, int ncampos)//Método con el que se insertan datos en una tabla
    {
        Statement sta;
        try 
        {
            sta = conn1.createStatement();
            String tcampos = "";
            String tvalores = "";
            for(int i = 0; i<ncampos; i++)
            {
                if(!valores[i].isEmpty())
                {
                    if((i+1)!=ncampos)
                    {
                        tcampos = tcampos + "`" + campos[i] + "`,";
                        tvalores = tvalores + "'" + valores[i] + "'" + ",";
                    }
                    else
                    {
                        tcampos = tcampos + "`" + campos[i] + "`";
                        tvalores = tvalores + "'" + valores[i] + "'";
                    }        
                }
                else
                {
                    if((i+1)!=ncampos)
                    {
                        tcampos = tcampos + "`" + campos[i] + "`,";
                        tvalores = tvalores + "NULL" + ",";
                    }
                    else
                    {
                        tcampos = tcampos + "`" + campos[i] + "`";
                        tvalores = tvalores + "NULL";
                    }
                }
            }
            sta.executeUpdate("INSERT INTO " + tabla + " (" + tcampos + ") VALUES (" + tvalores + ");");
            
            sta.close();
            return 0;
        } 
        catch (SQLException ex) 
        {
            return -1;
        }
    }
    
    public int modificaCampo(String tabla, String campo, String valorAntiguo, String valorNuevo, String id)//Método con el que se modifica un campo ya existente
    {
        Statement sta;
        try 
        {
            sta = conn1.createStatement();
            String pk = obtenerPK(tabla);
            String id_where= "";
            if(!id.equals(""))
            {
                id_where = " AND `" + pk + "` = '" + id + "'";
            }
            if(valorNuevo.equals(""))
            {
                sta.executeUpdate("UPDATE " + tabla + " SET `" + campo + "` = NULL WHERE `" + campo + "` = '" + valorAntiguo + "' " + id_where + ";");
            }
            else
            {
                if(valorAntiguo.equals("-"))
                {
                    sta.executeUpdate("UPDATE " + tabla + " SET `" + campo + "` = '" + valorNuevo + "' WHERE `" + campo + "` IS NULL " + id_where + ";");
                }
                else
                {
                    sta.executeUpdate("UPDATE " + tabla + " SET `" + campo + "` = '" + valorNuevo + "' WHERE `" + campo + "` = '" + valorAntiguo + "' " + id_where + ";");
                }
            } 
            sta.close();
            return 0;
        } 
        catch (SQLException ex) 
        {
            System.out.println("Error");
            return -1;
        }
    }
    
    public String obtenerPK(String tabla)//Método con el que se obtiene el nombre de la clave primaria de una tabla
    {
        Statement sta;
        String pk = "";
        try 
        {
            sta = conn1.createStatement();
            String query = "SELECT COLUMN_NAME as nombrepk FROM information_schema.COLUMNS WHERE (TABLE_NAME = '" + tabla + "') AND (COLUMN_KEY = 'PRI');";
            ResultSet rs = sta.executeQuery(query);
            if(rs.next())
            {
                pk = rs.getObject("nombrepk")+"";
            }
            rs.close();
            sta.close();
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return pk;
    }
    
    public String obtenerFK(String tabla, String campo)//Método usado para identificar la tabla a la que hace referencia la foreign key
    {
        Statement sta;
        String fktabla = "";
        try 
        {
            sta = conn1.createStatement();
            String query = "SELECT REFERENCED_TABLE_NAME as tabla FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '" + bbdd + "' AND TABLE_NAME = '" + tabla + "' AND COLUMN_NAME = '" + campo + "' AND NOT CONSTRAINT_NAME = 'PRIMARY';";
            ResultSet rs = sta.executeQuery(query);
            if(rs.next())
            {
                fktabla = rs.getObject("tabla")+"";
            }
            rs.close();
            sta.close();
            return fktabla;
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return fktabla+"";
    }
    
    public int añadeCampo(String tabla, String titulo, String tipo, String longitud, String nulo)//Método con el que se añade un nuevo campo a una tabla
    {
        Statement sta;
        try 
        {
            sta = conn1.createStatement();
            if(tipo.equals("DATE"))
            {
                sta.executeUpdate("ALTER TABLE " + tabla + " ADD " + titulo + " " + tipo + " " + nulo + ";");
            }
            else
            {
                sta.executeUpdate("ALTER TABLE " + tabla + " ADD " + titulo + " " + tipo + "(" + longitud + ") " + nulo + ";");
            }
            sta.close();
            return 0;
        } 
        catch (SQLException ex) 
        {
            return -1;
        }
    }
    
    public int borrarCampo(String campo, String tabla)//Método con el que se borra un campo específico de una tabla
    {
        Statement sta;
        try 
        {
            sta = conn1.createStatement();
            sta.executeUpdate("ALTER TABLE " + tabla + " DROP COLUMN `" + campo + "`");
            sta.close();
            return 0;
        } 
        catch (SQLException ex) 
        {
            return -1;
        }
    }
    
    public int borrarFila(String tabla, String campo, String valor, String id) //Método que permite borrar una fila elegida de una tabla 
    {
        Statement sta;
        try 
        {
            String pk = obtenerPK(tabla);
            String id_where= "";
            if(!id.equals(""))
            {
                id_where = " AND `" + pk + "` = '" + id + "'";
            }
            sta = conn1.createStatement();
            String query = "DELETE FROM " + tabla + " WHERE `" + campo + "` ='" + valor + "'" + id_where + ";";
            sta.executeUpdate(query);
            sta.close();
            return 0;
        } 
        catch (SQLException ex) 
        {
            return -1;
        }
    }
    
    public ResultSet realizaConsulta(String tablaElegida, String campo, String operacion, String valor)//Método con el que se realizan las consultas
    {
        Statement sta;
        ResultSet rs = null;
        try 
        {
            sta = conn1.createStatement();
            String query = "";
            if(campo.equals("") || campo.equals("*"))
            {
                query = "Select * from " + tablaElegida + "";  
            }
            else
            { 
                if(operacion.equals(">") || operacion.equals("<"))
                {
                    if(!valor.equals(""))
                    {
                        query = "SELECT * FROM " + tablaElegida + " WHERE `" + campo + "` " + operacion + valor + "";
                    }
                }
                else
                {
                    query = "SELECT * FROM " + tablaElegida + " WHERE `" + campo + "` LIKE '%" + valor + "%'";
                    System.out.println(query);
                }
                if(valor.equals("null") || valor.equals("NULL"))
                {
                    query = "Select * from " + tablaElegida + " WHERE `" + campo + "` IS NULL";  
                }
            }
            rs = sta.executeQuery(query);
            return rs;    
        } 
        catch (SQLException ex) 
        {
            rs = null;
        }
        return rs;
    }
    
    public String[] realizaConsultaCampo(String tablaElegida, String campo)//Método con el que se obtiene los valores de un campo especifico
    {
        Statement sta;
        String [] resultado = null;
        String pk = obtenerPK(tablaElegida);
        String query = "";
        try 
        {
            sta = conn1.createStatement();
            if(!pk.equals(""))
            {
                query = "SELECT `" + campo + "` as valor FROM " + tablaElegida + " ORDER BY `" + pk + "`;"; 
            }
            else
            {
                query = "SELECT `" + campo + "` as valor FROM " + tablaElegida + ";"; 
            } 
            ResultSet rs = sta.executeQuery(query);
            sta = null;
            
            //Mido la longitud de la consulta para formar el array
            sta = conn1.createStatement();
            String query2 = "SELECT COUNT(*) as longitud FROM " + tablaElegida + ";";
            ResultSet rs_count = sta.executeQuery(query2);
            rs_count.next();
            int nColumnas = rs_count.getInt("longitud");
            resultado = new String[nColumnas];
            
            //añado los valores que he sacado de la consulta al array
            int i = 0;
            while (rs.next())
            {
                resultado[i]=rs.getObject("valor")+"";
                i++;
            }
            return resultado;    
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return resultado;
    }
    
    public int cerrar_conexion()//Método con el que se cierra la conexión de la BBDD
    {
        try
        {
            System.out.println("Te has desconectado correctamente");
            conn1.close();
            return 0;
        }
        catch(SQLException ex)
        {
            System.out.println("ERROR");
            return -1;
        }
    }
}
