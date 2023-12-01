import data_cleaning

cleaning = data_cleaning.DataCleaning()
clean_data = cleaning.clean_user_data()
cleaning.upload_to_db(clean_data, "dim_users")
