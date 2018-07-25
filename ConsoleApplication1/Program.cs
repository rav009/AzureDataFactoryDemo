using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Configuration;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure;
using Microsoft.Azure.Management.DataFactories;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Common.Models;

using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
			string resourceGroupName = "PyWeiResourceGroup";
			string dataFactoryName = "PyWeiDataFactory";

			TokenCloudCredentials aadTokenCredentials = new TokenCloudCredentials(
				ConfigurationManager.AppSettings["SubscriptionId"],
				GetAuthorizationHeader().Result);

			Uri resourceManagerUri = new Uri(ConfigurationManager.AppSettings["ResourceManagerEndpoint"]);

			DataFactoryManagementClient client = new DataFactoryManagementClient(aadTokenCredentials, resourceManagerUri);

			//client.DataFactories.
			var pip = client.Pipelines.Get(resourceGroupName,dataFactoryName,"CopyActivity1");
			//Console.Write(pip.Pipeline.Name);
			//DateTime PipelineActivePeriodStartTime = new DateTime(2017, 5, 11, 0, 0, 0, 0, DateTimeKind.Utc);
			//DateTime PipelineActivePeriodEndTime = new DateTime(2017, 5, 12, 0, 0, 0, 0, DateTimeKind.Utc);
			SqlSource s = new SqlSource();
			s.SqlReaderQuery = "#3 the new query on 9.29 select column1 from atable";

			client.Pipelines.CreateOrUpdate(resourceGroupName, dataFactoryName,
				new PipelineCreateOrUpdateParameters()
				{
				    Pipeline = new Pipeline()
				    {
				        Name = pip.Pipeline.Name,
				        Properties = new PipelineProperties()
				        {
				            //Description = "Description",

				            // Initial value for pipeline's active period. With this, you won't need to set slice status
				            //Start = PipelineActivePeriodStartTime,
				            //End = PipelineActivePeriodEndTime,

				            Activities = new List<Activity>()
				            {
				                new Activity()
				                {
				                    Name = "PatientAct",
				                    Inputs = new List<ActivityInput>()
				                    {
				                        new ActivityInput() {
				                            Name = "PatientPayerODS"
				                        }
				                    },
				                    Outputs = new List<ActivityOutput>()
				                    {
				                        new ActivityOutput()
				                        {
				                            Name = "PatientPayerODS"
				                        }
				                    },
				                    TypeProperties = new CopyActivity()
				                    {
				                        Source = s,
				                        Sink = new SqlSink()
				                        {
				                            WriteBatchSize = 0,
				                            WriteBatchTimeout = TimeSpan.FromMinutes(0)
				                        }
				                    }
				                }
				            }
				        }
				    }
				});
			Console.WriteLine("Success!");
			Console.Read();
        }

		public static async Task<string> GetAuthorizationHeader()
		{
		    AuthenticationContext context = new AuthenticationContext(ConfigurationManager.AppSettings["ActiveDirectoryEndpoint"] + ConfigurationManager.AppSettings["ActiveDirectoryTenantId"]);
		    ClientCredential credential = new ClientCredential(
		        ConfigurationManager.AppSettings["ApplicationId"],
		        ConfigurationManager.AppSettings["Password"]);
		    AuthenticationResult result = await context.AcquireTokenAsync(
		        resource: ConfigurationManager.AppSettings["WindowsManagementUri"],
		        clientCredential: credential);
		
		    if (result != null)
		        return result.AccessToken;
		
		    throw new InvalidOperationException("Failed to acquire token");
		}
    }
}