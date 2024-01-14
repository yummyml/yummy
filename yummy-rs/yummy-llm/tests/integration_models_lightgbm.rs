use yummy_core::common::Result;
use yummy_llm::config::{LLMConfig, LLMEndpoint};
use yummy_llm::models::LLMModelFactory;

#[tokio::test]
async fn load_model_and_predict() -> Result<()> {
    let path = "../tests/llm/config.yaml".to_string();
    //let path = "../tests/mlflow/catboost_model/iris_my_model".to_string();
    let config = LLMConfig::new(&path).await?;
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
