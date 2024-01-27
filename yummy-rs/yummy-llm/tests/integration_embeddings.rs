use yummy_core::common::Result;
use yummy_llm::config::{LLMConfig, LLMEndpoint};
use yummy_llm::models::LLMModelFactory;

#[tokio::test]
async fn embeddings_e5() -> Result<()> {
    let path = "../tests/llm/config_embedding_e5.yaml".to_string();
    embeddings_test(path).await
}

#[tokio::test]
async fn embeddings_jinabert() -> Result<()> {
    let path = "../tests/llm/config_embedding_jinabert.yaml".to_string();
    embeddings_test(path).await
}

async fn embeddings_test(config_path: String) -> Result<()> {
    let config = LLMConfig::new(&config_path).await?;
    println!("{config:?}");

    let input = vec![
        String::from("Elixir of Eternal Twilight: Grants visions of realms beyond the veil."),
        String::from("Potion of Liquid Starlight: Imbues the drinker with celestial clarity."),
    ];

    let embeddings_config = config
        .endpoints
        .iter()
        .filter(|x| {
            matches!(
                x,
                LLMEndpoint::Embeddings {
                    metadata: _m,
                    spec: _s,
                }
            )
        })
        .last()
        .expect("Wrong configuration");

    let e5_model = LLMModelFactory::embedding_model(embeddings_config)?;
    let embeddings = e5_model.forward(input)?;

    println!("EMBEDDINGS: {embeddings:#?}");
    Ok(())
}

//cargo test --release -- --list   --show-output
//cargo test --release -- embeddings_e5 --show-output
