use buildernet_orderflow_proxy::{
    cli::OrderflowIngressArgs,
    init_tracing,
    runner::{CliContext, CliRunner},
};
use clap::Parser;

fn main() {
    let args = OrderflowIngressArgs::parse();
    init_tracing(args.log_json);

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create runtime");

    let runner = CliRunner::from_runtime(tokio_runtime);

    let command =
        |ctx: CliContext| buildernet_orderflow_proxy::run(OrderflowIngressArgs::parse(), ctx);

    if let Err(e) = runner.run_command_until_exit(command) {
        eprintln!("Orderflow proxy terminated with error: {e}");
    }
}
