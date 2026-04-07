use routines_core::server::RoutinesMcpServer;

pub fn cmd_serve() -> routines_core::error::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let server = RoutinesMcpServer::new();
        let service = rmcp::ServiceExt::serve(server, rmcp::transport::stdio())
            .await
            .map_err(|e| {
                routines_core::error::RoutineError::Io(std::io::Error::other(e.to_string()))
            })?;
        service.waiting().await.map_err(|e| {
            routines_core::error::RoutineError::Io(std::io::Error::other(e.to_string()))
        })?;
        Ok(())
    })
}
