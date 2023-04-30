use std::error::Error;

#[derive(thiserror::Error, Debug)]
pub enum SwapError {
    #[error("Wrong number of columns (required: {0}, provided: {1})")]
    ValidationWrongNumberOfColumns(usize, usize),

    #[error("Provided columns not match model requirements {0:?}")]
    ValidationWrongFeatureNames(Vec<String>),
}

fn find_swap(feature_names: &[String], columns: &[String]) -> Option<(usize, usize)> {
    let num = columns.len();
    for i in 0..num {
        let col = &columns[i];
        if col != &feature_names[i] {
            for (j, fcol) in feature_names.iter().enumerate().take(num).skip(i) {
                if col == fcol {
                    return Some((i, j));
                }
            }
        }
    }

    None
}

pub fn find_swaps(
    feature_names: &Vec<String>,
    mut columns: Vec<String>,
) -> Result<Vec<(usize, usize)>, Box<dyn Error>> {
    let mut swaps = Vec::new();

    if feature_names.len() != columns.len() {
        return Err(Box::new(SwapError::ValidationWrongNumberOfColumns(
            feature_names.len(),
            columns.len(),
        )));
    }

    let mut f1 = feature_names.clone();
    let mut f2 = columns.clone();
    f1.sort();
    f2.sort();
    if f1.iter().zip(&f2).filter(|&(a, b)| a == b).count() != f1.len() {
        return Err(Box::new(SwapError::ValidationWrongFeatureNames(
            feature_names.clone(),
        )));
    }

    while let Some(swap) = find_swap(feature_names, &columns) {
        columns.swap(swap.0, swap.1);
        swaps.push(swap);
    }

    Ok(swaps)
}

pub fn reorder(
    feature_names: &Vec<String>,
    columns: Vec<String>,
    mut numeric_features: Vec<Vec<f64>>,
) -> Result<Vec<Vec<f64>>, Box<dyn Error>> {
    let num_rows = numeric_features.len();
    let swaps = find_swaps(feature_names, columns)?;
    for numeric_feature in numeric_features.iter_mut().take(num_rows) {
        for swap in &swaps {
            numeric_feature.swap(swap.0, swap.1);
        }
    }

    Ok(numeric_features)
}

#[test]
fn test_matching() {
    let mut a = ["1", "2", "3", "4", "5"];
    let mut b = ["1", "3", "2", "4", "5"];
    a.sort();
    b.sort();

    let matching = a.iter().zip(&b).filter(|&(a, b)| a == b).count();
    assert_eq!(matching, a.len());
}

#[test]
fn test_reorder() -> Result<(), Box<dyn Error>> {
    let mut columns = Vec::new();
    columns.push("3".to_string());
    columns.push("1".to_string());
    columns.push("2".to_string());
    columns.push("0".to_string());

    let mut feature_names = Vec::new();
    feature_names.push("0".to_string());
    feature_names.push("1".to_string());
    feature_names.push("2".to_string());
    feature_names.push("3".to_string());

    let mut data = Vec::new();
    let mut numeric_features = Vec::new();
    numeric_features.push(3.0);
    numeric_features.push(1.0);
    numeric_features.push(2.0);
    numeric_features.push(0.0);
    data.push(numeric_features.clone());
    data.push(numeric_features.clone());

    data = reorder(&feature_names, columns, data)?;

    assert_eq!(data[0][0], 0.0);
    assert_eq!(data[0][1], 1.0);
    assert_eq!(data[0][2], 2.0);
    assert_eq!(data[0][3], 3.0);

    assert_eq!(data[1][0], 0.0);
    assert_eq!(data[1][1], 1.0);
    assert_eq!(data[1][2], 2.0);
    assert_eq!(data[1][3], 3.0);

    Ok(())
}

#[test]
fn test_find_swaps() -> Result<(), Box<dyn Error>> {
    let mut columns = Vec::new();
    columns.push("12".to_string());
    columns.push("1".to_string());
    columns.push("2".to_string());
    columns.push("3".to_string());
    columns.push("4".to_string());
    columns.push("6".to_string());
    columns.push("5".to_string());
    columns.push("7".to_string());
    columns.push("8".to_string());
    columns.push("9".to_string());
    columns.push("10".to_string());
    columns.push("11".to_string());
    columns.push("0".to_string());

    let mut feature_names = Vec::new();
    feature_names.push("0".to_string());
    feature_names.push("1".to_string());
    feature_names.push("2".to_string());
    feature_names.push("3".to_string());
    feature_names.push("4".to_string());
    feature_names.push("5".to_string());
    feature_names.push("6".to_string());
    feature_names.push("7".to_string());
    feature_names.push("8".to_string());
    feature_names.push("9".to_string());
    feature_names.push("10".to_string());
    feature_names.push("11".to_string());
    feature_names.push("12".to_string());

    let swaps = find_swaps(&feature_names, columns)?;

    assert_eq!(swaps.len(), 2);
    assert_eq!(swaps[0], (0, 12));
    assert_eq!(swaps[1], (5, 6));

    Ok(())
}

#[test]
#[should_panic]
fn test_find_swaps_panic() {
    let mut columns = Vec::new();
    columns.push("12".to_string());
    columns.push("1".to_string());

    let mut feature_names = Vec::new();
    feature_names.push("0".to_string());
    feature_names.push("1".to_string());

    let swaps = find_swaps(&feature_names, columns).unwrap();

    assert_eq!(swaps.len(), 1);
}

#[test]
#[should_panic]
fn test_find_swaps_panic_len() {
    let mut columns = Vec::new();
    columns.push("12".to_string());
    columns.push("1".to_string());
    columns.push("2".to_string());

    let mut feature_names = Vec::new();
    feature_names.push("0".to_string());
    feature_names.push("1".to_string());

    let swaps = find_swaps(&feature_names, columns).unwrap();

    assert_eq!(swaps.len(), 1);
}
