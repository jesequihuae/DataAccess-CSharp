using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Web;

namespace Data
{
    public class DataAccess
    {
        /*  CONSTRUCTOR VACÍO */
        public DataAccess() { }

        /****************************************
         *              CONEXIONES              * 
         ***************************************/

        /* RETORNA LA CONEXIÓN POR DEFAULT EN WEBCONFIG CON dbConn */
        private String connectionString()
        {
            return ConfigurationManager.ConnectionStrings["NEXTADMIN_CONECTION"].ToString();
        }

        /* RETORNA UNA CONEXION DIFERENTE */
        private String connectionString(String Connection)
        {
            return ConfigurationManager.ConnectionStrings[Connection].ToString();
        }

        /****************************************
         *              DATASET                 * 
         ***************************************/

        /* EJECUTA STORED PROCEDURE Y RETORNA DATASET */
        public DataSet executeStoreProcedureDataSet(String spName)
        {

            SqlConnection dbconn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            try
            {
                dbconn.Open();
                sqlDA.Fill(ds);
                dbconn.Close();
                sqlDA.Dispose();
                cmd.Dispose();

                for (int i = 0; i < ds.Tables.Count; ++i)
                {
                    ds.Tables[i].TableName = string.Format("table{0}", i + 1);
                }

                return ds;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE Y RETORNA DATASET EN UNA CONEXION DIFERENTE */
        public DataSet executeStoreProcedureDataSet(String spName, String Connection)
        {

            SqlConnection dbconn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            try
            {
                dbconn.Open();
                sqlDA.Fill(ds);
                dbconn.Close();
                sqlDA.Dispose();
                cmd.Dispose();

                for (int i = 0; i < ds.Tables.Count; ++i)
                {
                    ds.Tables[i].TableName = string.Format("table{0}", i + 1);
                }

                return ds;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS Y RETORNA DATASET */
        public DataSet executeStoreProcedureDataSet(String spName, Dictionary<String, object> parameters)
        {

            SqlConnection dbconn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, parameters[item]);
            }

            try
            {
                dbconn.Open();
                sqlDA.Fill(ds);
                dbconn.Close();
                sqlDA.Dispose();
                cmd.Dispose();

                for (int i = 0; i < ds.Tables.Count; ++i)
                {
                    ds.Tables[i].TableName = string.Format("table{0}", i + 1);
                }

                return ds;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS Y RETORNA DATASET EN UNA CONEXION DIFERENTE */
        public DataSet executeStoreProcedureDataSet(String spName, Dictionary<String, object> parameters, String Connection)
        {

            SqlConnection dbconn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataSet ds = new DataSet();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, parameters[item]);
            }

            try
            {
                dbconn.Open();
                sqlDA.Fill(ds);
                dbconn.Close();
                sqlDA.Dispose();
                cmd.Dispose();

                for (int i = 0; i < ds.Tables.Count; ++i)
                {
                    ds.Tables[i].TableName = string.Format("table{0}", i + 1);
                }

                return ds;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }


        /****************************************
         *              DATATABLE               * 
         ***************************************/


        /* EJECUTA STORED PROCEDURE Y RETORNA UN DATATABLE */
        public DataTable executeStoredProcedureDataTable(String StoredProcedure)
        {
            SqlConnection dbConn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(StoredProcedure, dbConn);

            SqlDataAdapter sqlAdp = new SqlDataAdapter(cmd);
            DataTable dtx = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            try
            {
                dbConn.Open();
                sqlAdp.Fill(dtx);
                dbConn.Close();
                return dtx;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbConn.State != ConnectionState.Closed)
                {
                    dbConn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE Y RETORNA UN DATATABLE EN UNA CONEXION DIFERENTE */
        public DataTable executeStoredProcedureDataTable(String StoredProcedure, String Connection)
        {
            SqlConnection dbConn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(StoredProcedure, dbConn);

            SqlDataAdapter sqlAdp = new SqlDataAdapter(cmd);
            DataTable dtx = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            try
            {
                dbConn.Open();
                sqlAdp.Fill(dtx);
                dbConn.Close();
                return dtx;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbConn.State != ConnectionState.Closed)
                {
                    dbConn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS Y RETORNA UN DATATABLE */
        public DataTable executeStoredProcedureDataTable(String StoredProcedure, Dictionary<String, Object> Parameters)
        {
            SqlConnection dbConn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(StoredProcedure, dbConn);

            SqlDataAdapter sqlAdp = new SqlDataAdapter(cmd);
            DataTable dtx = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in Parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, Parameters[item]);
            }

            try
            {
                dbConn.Open();
                sqlAdp.Fill(dtx);
                dbConn.Close();
                return dtx;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbConn.State != ConnectionState.Closed)
                {
                    dbConn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS Y RETORNA UN DATATABLE EN UNA CONEXION DIFERENTE */
        public DataTable executeStoredProcedureDataTable(String StoredProcedure, Dictionary<String, Object> Parameters, String Connection)
        {
            SqlConnection dbConn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(StoredProcedure, dbConn);

            SqlDataAdapter sqlAdp = new SqlDataAdapter(cmd);
            DataTable dtx = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in Parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, Parameters[item]);
            }

            try
            {
                dbConn.Open();
                sqlAdp.Fill(dtx);
                dbConn.Close();
                return dtx;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbConn.State != ConnectionState.Closed)
                {
                    dbConn.Close();
                }
            }
        }


        /****************************************
         *              NOTHING                 * 
         ***************************************/


        /* EJECUTA STORED PROCEDURE Y NO RETORNA NADA */
        public void executeStoreProcedureNonQuery(String StoredProcedure)
        {
            SqlConnection dbconn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(StoredProcedure, dbconn);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;
            try
            {
                dbconn.Open();
                cmd.ExecuteNonQuery();
                dbconn.Close();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE Y NO RETORNA NADA EN UNA CONEXION DIFERENTE*/
        public void executeStoreProcedureNonQuery(String StoredProcedure, String Connection)
        {
            SqlConnection dbconn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(StoredProcedure, dbconn);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;
            try
            {
                dbconn.Open();
                cmd.ExecuteNonQuery();
                dbconn.Close();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS SIN RETORNAR NADA */
        public void executeStoreProcedureNonQuery(String StoredProcedure, Dictionary<String, object> parameters)
        {
            SqlConnection dbconn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(StoredProcedure, dbconn);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, parameters[item]);
            }

            try
            {
                dbconn.Open();
                cmd.ExecuteNonQuery();
                dbconn.Close();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS EN CONEXION DIFERENTE */
        public void executeStoreProcedureNonQuery(String StoredProcedure, Dictionary<String, object> parameters, String Connection)
        {
            SqlConnection dbconn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(StoredProcedure, dbconn);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, parameters[item]);
            }

            try
            {
                dbconn.Open();
                cmd.ExecuteNonQuery();
                dbconn.Close();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }


        /****************************************
         *               FLOAT                  * 
         ***************************************/


        /* EJECUTA STORED PROCEDURE Y RETORNA UN FLOTANTE */
        public float executeStoreProcedureFloat(String spName)
        {

            SqlConnection dbconn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            try
            {
                dbconn.Open();
                sqlDA.Fill(dt);
                dbconn.Close();
                return float.Parse(dt.Rows[0][0].ToString());
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE Y RETORNA UN FLOTANTE EN UNA CONEXION DIFERENTE */
        public float executeStoreProcedureFloat(String spName, String Connection)
        {

            SqlConnection dbconn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            try
            {
                dbconn.Open();
                sqlDA.Fill(dt);
                dbconn.Close();
                return float.Parse(dt.Rows[0][0].ToString());
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS Y RETORNA UN FLOTANTE */
        public float executeStoreProcedureFloat(String spName, Dictionary<String, object> parameters)
        {

            SqlConnection dbconn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, parameters[item]);
            }

            try
            {
                dbconn.Open();
                sqlDA.Fill(dt);
                dbconn.Close();
                return float.Parse(dt.Rows[0][0].ToString());
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS Y RETORNA UN FLOTANTE EN CONEXION DIFERENTE */
        public float executeStoreProcedureFloat(String spName, Dictionary<String, object> parameters, String Connection)
        {
            SqlConnection dbconn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, parameters[item]);
            }

            try
            {
                dbconn.Open();
                sqlDA.Fill(dt);
                dbconn.Close();
                return float.Parse(dt.Rows[0][0].ToString());
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }


        /****************************************
         *                 INT                  * 
         ***************************************/


        /* EJECUTA STORED PROCEDURE Y RETORNA UN ENTERO */
        public int executeStoreProcedureInt(String spName)
        {

            SqlConnection dbconn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            try
            {
                dbconn.Open();
                sqlDA.Fill(dt);
                dbconn.Close();
                return int.Parse(dt.Rows[0][0].ToString());
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE Y RETORNA UN ENTERO EN UNA CONEXION DIFERENTE */
        public int executeStoreProcedureInt(String spName, String Connection)
        {

            SqlConnection dbconn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            try
            {
                dbconn.Open();
                sqlDA.Fill(dt);
                dbconn.Close();
                return int.Parse(dt.Rows[0][0].ToString());
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS Y RETORNA UN ENTERO */
        public int executeStoreProcedureInt(String spName, Dictionary<String, object> parameters)
        {

            SqlConnection dbconn = new SqlConnection(connectionString());
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, parameters[item]);
            }

            try
            {
                dbconn.Open();
                sqlDA.Fill(dt);
                dbconn.Close();
                return int.Parse(dt.Rows[0][0].ToString());
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

        /* EJECUTA STORED PROCEDURE CON PARAMETROS Y RETORNA UN ENTERO EN CONEXION DIFERENTE */
        public int executeStoreProcedureInt(String spName, Dictionary<String, object> parameters, String Connection)
        {
            SqlConnection dbconn = new SqlConnection(connectionString(Connection));
            SqlCommand cmd = new SqlCommand(spName, dbconn);

            SqlDataAdapter sqlDA = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();

            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 1000;

            foreach (string item in parameters.Keys)
            {
                cmd.Parameters.AddWithValue(item, parameters[item]);
            }

            try
            {
                dbconn.Open();
                sqlDA.Fill(dt);
                dbconn.Close();
                return int.Parse(dt.Rows[0][0].ToString());
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (dbconn.State != ConnectionState.Closed)
                {
                    dbconn.Close();
                }
            }
        }

    }
}