use buildernet_orderflow_ingress::cli::OrderflowIngressArgs;
use clap::Parser;

#[tokio::main]
async fn main() {
    // When a task crashes, we crash the whole program.
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        std::process::exit(1);
    }));

    if let Err(error) = buildernet_orderflow_ingress::run(OrderflowIngressArgs::parse()).await {
        eprintln!("Error: {error:?}");
        std::process::exit(1);
    }
}
