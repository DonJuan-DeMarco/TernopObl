using System;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;

namespace TestingProj
{
    public static class Program
    {
        public static void Main()
        {
            var dt = GetDataTableFromDbf("../../../dbase_83.dbt");

            //const string connString = @"Data Source=(localdb)\MSSQLLocalDB;Integrated Security=true;";
            //const string connString = @"Data Source=DESKTOP-HRPL5CJ\\SQLEXPRESS;Integrated Security=True";
            //const string connString = @"Data Source=.\SQLEXPRESS;Integrated Security=True";

            const string connString = "Data Source=DESKTOP-HRPL5CJ\\SQLEXPRESS;Initial Catalog=OblEnergo;Integrated Security=True";
            SaveDataTableToDatabase(dt, connString);
        }

        [SuppressMessage("Interoperability", "CA1416:Validate platform compatibility",
            Justification = "<Pending>")]
        private static DataTable GetDataTableFromDbf(string filepath,
            string dbaseType = "dBASE III")
        {
            filepath = Path.GetFullPath(filepath, Directory.GetCurrentDirectory());
            var path = Path.GetDirectoryName(filepath);
            var filename = Path.GetFileName(filepath);
            DataTable table = new();


            
            var connectionHandler =
                new OleDbConnection(
                    $"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={path};Extended Properties={dbaseType};");
            connectionHandler.Open();

            if (connectionHandler.State != ConnectionState.Open) throw new Exception("Can't open dbase file");
            var mySql = $"select * from {filename}";

            var myQuery = new OleDbCommand(mySql, connectionHandler);
            var da = new OleDbDataAdapter(myQuery);

            da.Fill(table);

            connectionHandler.Close();

            table.TableName = Path.GetFileNameWithoutExtension(filename);

            return table;
        }

        private static void SaveDataTableToDatabase(DataTable dataTable, string connectionString)
        {
            using var connection = new SqlConnection(connectionString);
            connection.Open();
            using var bulkCopy = new SqlBulkCopy(connection);
            foreach (DataColumn c in dataTable.Columns)
                bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);

            var createTableBuilder = new StringBuilder("CREATE TABLE [" + dataTable.TableName + "]");
            createTableBuilder.AppendLine("(");

            foreach (DataColumn dc in dataTable.Columns)
                createTableBuilder.AppendLine("  [" + dc.ColumnName + "] VARCHAR(MAX),");

            createTableBuilder.Remove(createTableBuilder.Length - 1, 1);
            createTableBuilder.AppendLine(")");

            var createTableCommand = new SqlCommand(createTableBuilder.ToString(), connection);
            createTableCommand.ExecuteNonQuery();

            bulkCopy.DestinationTableName = dataTable.TableName;
            try
            {
                bulkCopy.WriteToServer(dataTable);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}