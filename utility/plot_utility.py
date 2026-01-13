import seaborn as sns
import matplotlib.pyplot as plt

class KernelPlot:
    def __init__(self, data1, column1, label1, data2=None, column2=None, label2=None):
        self.data1 = data1
        self.column1 = column1
        self.label1 = label1
        self.data2 = data2
        self.column2 = column2
        self.label2 = label2

    def plot_density(self):
        plt.figure(figsize=(8, 5))
        sns.kdeplot(self.data1[self.column1], fill=True, color='steelblue', label=f'{self.label1}', alpha=0.6)
        if self.data2 is not None and self.column2 is not None:
            sns.kdeplot(self.data2[self.column2], fill=True, color='darkorange', label=f'{self.label2}', alpha=0.6)
        plt.xlabel("Cosine Similarity")
        plt.ylabel("Density")
        plt.legend()
        plt.tight_layout()
        plt.show()
