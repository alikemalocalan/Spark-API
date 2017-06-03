import pandas as pd


class Mining:
    def __init__(self):
        self.user_df = pd.read_csv('dataset/userid-profile.tsv', delimiter='\t', error_bad_lines=False)

    def user_counter(self):
        return pd.Series(self.user_df['#id'].unique().shape[0]).to_json()

    def mean_users_age(self):
        return pd.Series("%.2f" % self.user_df['age'].mean()).to_json()

    def gender_counter_male_female(self):
        return pd.Series(self.user_df['gender'].value_counts()).to_json()

    def counter_user_country_by_country(self):
        return pd.Series(self.user_df['country'].value_counts()).to_json()

    def country_counter(self):
        return pd.Series(self.user_df['country'].unique().shape[0]).to_json()

    def age_mean_country_by_country(self):
        return pd.Series(({"%.1f"}, self.user_df.groupby(['country'])['age'].mean())).to_json()

    def gender_counter_country_by_country(self):
        return pd.Series(self.user_df.groupby(['country'])['gender'].value_counts()).to_json()


mining = Mining()
