import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_curve, precision_recall_curve

def CorrelationMatrix(columns, df, categ):
    '''Creates a correlation matrix between specific columns.'''

    corr_matrix = df[columns].corr()

    fig, ax = plt.subplots(figsize=(20,20))
    # Visualize the correlation matrix using a heatmap
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm')

    # Save the heatmap as a jpeg file
    plt.savefig(f'correlation_matrix_{categ}.jpg', dpi=300, bbox_inches= 'tight')


def dropColumnsAfterCorrMatrix(columnsToDrop, df):
    '''After analyzing correlation matrices, it drops some columns that had no use.'''

    df = df.drop(columnsToDrop, axis=1)
    return df


def ConfusionMatrix(yTest, yPred, algorithm):
    '''Creates a confusion matrix for each model.'''
    
    # create the confusion matrix
    cm = confusion_matrix(yTest, yPred)

    fig, ax = plt.subplots(figsize=(15,15))
    # create a heatmap of the confusion matrix
    sns.heatmap(cm, annot=True, cmap='Blues', fmt='g')
    
    # set the axis labels
    plt.xlabel('Predicted')
    plt.ylabel('Actual')

    plt.savefig(f'confusion_matrix_{algorithm}.jpg', dpi=300, bbox_inches='tight')


def ModelsAccuracyHist(ac):
    '''Creates a histogram that contains the accuracy score for each model.'''

    cl = ['Random Forest', 'AdaBoost', 'SVM', 'KNN', 'XGBoost']
    # Create a histogram
    fig, ax = plt.subplots()
    bars = ax.barh(cl, ac, color='blue', alpha=0.8)

    # Find the index of the biggest bar
    max_count_index = ac.index(max(ac))

    # Change the color of the biggest bar to red
    bars[max_count_index].set_color('purple')

    # Add axis labels and title
    ax.set_xlabel('Accuracy', fontsize=12, fontweight='bold')
    ax.set_ylabel('Classifier', fontsize=12, fontweight='bold')
    ax.set_title('Accuracy of classifiers', fontsize=14, fontweight='bold')

    # Customize the axis ticks
    ax.tick_params(axis='both', which='major', labelsize=10)
    ax.tick_params(axis='both', which='minor', labelsize=8)

    # Remove the top and right spines
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    # Add a grid
    ax.grid(axis='x', linestyle='--', alpha=0.5, color = 'black')

    # Add text labels to each bar
    for i, bar in enumerate(bars):
        ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height() / 2, ac[i],
            ha='left', va='center', fontsize=10)

    plt.savefig(f'Accuracy_of_classifiers.jpg', dpi=300, bbox_inches='tight')

def ModelsF1ScoreHist(ac):
    '''Creates a histogram that contains the F1 score for each model.'''

    cl = ['Random Forest', 'AdaBoost', 'SVM', 'KNN', 'XGBoost']
    # Create a histogram
    fig, ax = plt.subplots()
    bars = ax.barh(cl, ac, color='blue', alpha=0.8)

    # Find the index of the biggest bar
    max_count_index = ac.index(max(ac))

    # Change the color of the biggest bar to red
    bars[max_count_index].set_color('purple')

    # Add axis labels and title
    ax.set_xlabel('F1 Score', fontsize=12, fontweight='bold')
    ax.set_ylabel('Classifier', fontsize=12, fontweight='bold')
    ax.set_title('F1 scores of classifiers', fontsize=14, fontweight='bold')

    # Customize the axis ticks
    ax.tick_params(axis='both', which='major', labelsize=10)
    ax.tick_params(axis='both', which='minor', labelsize=8)

    # Remove the top and right spines
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    # Add a grid
    ax.grid(axis='x', linestyle='--', alpha=0.5, color = 'black')

    # Add text labels to each bar
    for i, bar in enumerate(bars):
        ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height() / 2, ac[i],
            ha='left', va='center', fontsize=10)

    plt.savefig(f'F1Score_of_classifiers.jpg', dpi=300, bbox_inches='tight')