using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Web;

namespace Data
{
    /// <summary>
    ///  Class to access to your data using methods
    /// </summary>
    public class DataAccess
    {
        /// <summary>
        ///  Empty constructor
        /// </summary>
        public DataAccess() { }
      
        /// <summary>
        ///     Returns connection using connection string 
        /// </summary>
        /// <returns></returns>
        private String connectionString()
        {
            return ConfigurationManager.ConnectionStrings["CONNECTION_NAME"].ToString();
        }

        /// <summary>
        ///     Returns connection receiving the connection string name
        /// </summary>
        /// <param name="Connection"></param>
        /// <returns></returns>
        private String connectionString(String Connection)
        {
            return ConfigurationManager.ConnectionStrings[Connection].ToString();
        }

        /****************************************
         *              DATASET                 * 
         ***************************************/
         
        /// <summary>
        ///     Execute stored procedure without params and returns DataSet
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <returns></returns>
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
        
        /// <summary>
        ///     Execute stored procedure without params using diferent connection string and returns DataSet
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="Connection">ConnectionString name</param>
        /// <returns></returns>
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
        
        /// <summary>
        ///     Execute stored procedure with params and returns DataSet
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="parameters">Dictionary with params</param>
        /// <returns></returns>
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
    
        /// <summary>
        ///     Execute stored procedure with params using specific connection returns DataSet
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="parameters">Dictionary with params</param>
        /// <param name="Connection">ConnectionString name</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure without params and returns DataTable
        /// </summary>
        /// <param name="StoredProcedure">Stored procedure name</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure without params using specific connection returns DataTable
        /// </summary>
        /// <param name="StoredProcedure">Stored procedure name</param>
        /// <param name="Connection">ConnectionString name</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure with params and returns DataTable
        /// </summary>
        /// <param name="StoredProcedure">Stored Procedure name</param>
        /// <param name="Parameters">Dictionary with params</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure with params using specific connection returns DataTable
        /// </summary>
        /// <param name="StoredProcedure">Stored procedure name</param>
        /// <param name="Parameters">Dictionary with params</param>
        /// <param name="Connection">ConnectionString name</param>
        /// <returns></returns>
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
         
        /// <summary>
        ///     Execute stored procedure returns nothing
        /// </summary>
        /// <param name="StoredProcedure">Stored procedure name</param>
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

        /// <summary>
        ///     Execute stored procedure using specific connection returns nothing
        /// </summary>
        /// <param name="StoredProcedure">Stored procedure name</param>
        /// <param name="Connection">ConnectionString name</param>
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
        
        /// <summary>
        ///     Execute stored procedure with params returns nothing
        /// </summary>
        /// <param name="StoredProcedure"></param>
        /// <param name="parameters"></param>
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

        /// <summary>
        ///     Execute stored procedure with params using specific connection returns nothing
        /// </summary>
        /// <param name="StoredProcedure">Stored procedure name</param>
        /// <param name="parameters">Dictionary with params</param>
        /// <param name="Connection">ConnectionString name</param>
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

        /// <summary>
        ///     Execute stored procedure with no params returns float
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure with no params using specific connection returns float
        /// </summary>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="Connection">ConnectionString name</param>
        /// <returns></returns>
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
        
        /// <summary>
        ///     Execute stored procedure with params returns float
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="parameters">Dictionary with params</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure with params using specific connection returns float
        /// </summary>
        /// <param name="spName">Stored Procedure name</param>
        /// <param name="parameters">Dictionary with params</param>
        /// <param name="Connection">ConnectionString name</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure with no params returns int
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure with no params using specific connection returns int
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="Connection">ConnectionString name</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure with params returns int
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="parameters">Dictionary with params</param>
        /// <returns></returns>
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

        /// <summary>
        ///     Execute stored procedure with params using specific connection returns int
        /// </summary>
        /// <param name="spName">Stored procedure name</param>
        /// <param name="parameters">Dictionary with params</param>
        /// <param name="Connection">ConnectionString name</param>
        /// <returns></returns>
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