# Entity Mapping
Entity Resolution has a number of different options for matching entities. Various "brute force" or manual options exist, but they are not scalable.

Due to the uncertainty of the data, we will be using a probabilistic approach to match the entities. We will be using the dedupe library to map relationships.
Dedupe has the advantage of having a build in active learning component. This allows us to train a model and then use the model to predict the matches.

The following is a brief outline of the steps we will be taking to map the entities.

## Preprocessing
Considering the data is coming from two different sources, we need to preprocess the data to make sure the data is in the same format.

After combining the a__ datasets on the geo_id column and the b__ datasets on the b_entity_id column, we will drop various columns to create two pared down datasets. After which, we will be using the following preprocessing steps:
- Remove special characters
- Remove spaces
- Remove extra whitespaces
- Remove any newlines
- Format US and CA postal codes. (The other countries will be ignored for now)

## Training
We will be using the dedupe library to train a model to match the entities. The training will be done in two steps:
- Training the model
- Active learning

### Training the model
The training will be done using the following steps:
- Create a training dataset
- Create a dedupe variable definition
- Train the model
- Review the results
- Save the model

#### Create a training dataset
The training dataset will be created by sampling the combined dataset. The sampling will be done by selecting a random sample of 1500 records from the combined dataset. The sample will be split into two datasets, one for training and one for testing.

## Matching
The matching will be done using the following steps in the main function:
- Load the model
- Load the data
- Run the model and get the results

The means to calculate the string difference by dedupe is an Affine Gap Distance. There are various others that could be employed, such as Levendstein Distance, Jaro Distance, etc. The Affine Gap Distance is a good choice for this dataset as it is a good balance between speed and accuracy.

## Scaling the model
A more robust model train and predict pipeline could start with a Naive Bayes classifier to determine if the records are a match or not. Recent research has shown the advantage of using a Naive Bayes appraoch (see for example "Statistical Approaches for Entity
Resolution under Uncertainty," Marchant, Niel, 2021). Other possibilities would be a Random Forest or a Support Vector Machine, SVMs becoming more popular in recent years.

Overall, however, the largest hurdles are the data processing. Depending on where the data lives, how it is stores, cleaned, staged, etc will determine trade-offs. Having a clean training set is one of the most important parts of the process.

Entity resolution typically follows this process flow:

![Entity Resolution Process Flow](![Entity Resolution - Process Flow.png](data%2FEntity%20Resolution%20-%20Process%20Flow.png)

## Model Training
The model training could be done in Airflow using Astronomer to manage the environments. A basic sketch of that architecture is below:

![Entity Resolution - Model Training](!![Entity Resolution - Model.png](data%2FEntity%20Resolution%20-%20Model.png))
