using System;
using System.Data;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Data.SqlClient;

namespace SynapseConnection
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Key Vault Configuration
            const string keyVaultUri = "https://anc-aems-kv-westus2-adt.vault.azure.net/";
            const string usernameSecretName = "rad-app-accountname-nonprod-adt";
            const string passwordSecretName = "rad-app-accountsecret-nonprod-adt";

            string username, password;

            try
            {
                // Fetch secrets from Azure Key Vault
                var secretClient = new SecretClient(new Uri(keyVaultUri), new DefaultAzureCredential());
                var usernameSecret = await secretClient.GetSecretAsync(usernameSecretName);
                var passwordSecret = await secretClient.GetSecretAsync(passwordSecretName);

                username = usernameSecret.Value.Value;
                password = passwordSecret.Value.Value;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error retrieving secrets: {ex.Message}");
                return;
            }

            // Database connection string
            string connectionString = $"Server=prd-ghs-synapse.sql.azuresynapse.net;Database=prd_ghs_synapse_pool;User ID={username};Password={password};";

            using (var connection = new SqlConnection(connectionString))
            {
                try
                {
                    Console.WriteLine("Connecting to Azure Synapse...");
                    await connection.OpenAsync();
                    Console.WriteLine("Connected!");

                    // Call the stored procedure
                    string storedProcedure = "[GHS_DW].[SP_RAD_GET_CPIR_FIELDS]";
                    using (var command = new SqlCommand(storedProcedure, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;

                        // Add parameters
                        command.Parameters.AddWithValue("@WHSE", "Warehouse1");
                        command.Parameters.AddWithValue("@RX_NUMBER", 12345);
                        command.Parameters.AddWithValue("@RX_TX_WILL_CALL_PICKED_UP_DATE_YYYYMMDD", "20231201");
                        command.Parameters.AddWithValue("@RX_TX_TX_NUMBER", 67890);

                        // Execute the command and process results
                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                Console.WriteLine($"Result: {reader[0]}");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Database error: {ex.Message}");
                }
                finally
                {
                    await connection.CloseAsync();
                    Console.WriteLine("Connection closed.");
                }
            }
        }
    }
}
