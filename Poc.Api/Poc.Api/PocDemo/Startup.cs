using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using GraphiQl;
using GraphQL;
using GraphQL.Http;
using GraphQL.Types;
using GraphQL.Server.Transports.AspNetCore;
using GraphQL.Server.Transports.WebSockets;
using GraphQL.Server.Transports.Subscriptions.Abstractions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using PoC.Api;
using PoC.Api.GraphQL;
using PoC.Api.GraphQL.Queries;
using PoC.Api.GraphQL.Types;
using PoC.Data.Model;
using PoC.Data.Repository;
using PoC.Data.Service;
using GraphQL.Server;
using GraphQL.Server.Ui.Voyager;
using GraphQL.Server.Ui.Playground;

namespace Poc.Api
{
    public class Startup
    {
        public object Environment { get; private set; }

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {

            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1);

            services.AddSingleton<IDependencyResolver>(s => new FuncDependencyResolver(s.GetRequiredService));
            services.AddSingleton<IDocumentExecuter, DocumentExecuter>();
            services.AddSingleton<IDocumentWriter, DocumentWriter>();
            services.AddSingleton<PoCSchema>();

            services.AddSingleton<MessageQuery>();

            services.AddSingleton<IMessageService, MessageService>();

            services.AddSingleton<Message>();
            services.AddSingleton<PoC.Api.GraphQL.Types.MessageType>();

            //services.AddGraphQLHttp();
            //services.AddGraphQLWebSocket<PoCSchema>();

            services.AddGraphQL(options =>
            {
                options.EnableMetrics = true;
                options.ExposeExceptions = true;
            });

            services.AddSingleton<ICassandraRepository, CassandraRepository>();
            services.AddMvc();
            services.AddCors(options => options.AddPolicy("AllowAllOrigins", builder =>
          builder.AllowAnyOrigin()
          .AllowAnyMethod().
          AllowCredentials()
          .AllowAnyHeader()));
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            
            app.UseDefaultFiles();

            app.UseStaticFiles();

            app.UseWebSockets();

            app.UseCors("AllowAllOrigins");;

            // use HTTP middleware for ChatSchema at path /graphql
            app.UseGraphQL<PoCSchema>("/graphql");

            app.UseGraphQLPlayground(new GraphQLPlaygroundOptions()
            {
                Path = "/ui/playground"
            });

            app.UseGraphiQLServer(new GraphQL.Server.Ui.GraphiQL.GraphiQLOptions
            {
                GraphiQLPath = "/graphiql",
                GraphQLEndPoint = "/graphql"
            });

            app.UseGraphQLVoyager(new GraphQLVoyagerOptions()
            {
                GraphQLEndPoint = "/graphql",
                Path = "/ui/voyager"
            });
            app.UseMvc();

        }
    }
}
