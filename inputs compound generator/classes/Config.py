class Config:

    def __init__(self, id_config, config_name, postgre_dbname, postgre_user, postgre_url, postgre_pass, eu_db, dp_pop_db, wwtp_con_db, comp_con_db):
        self.config_id = id_config
        self.config_name = config_name
        self.postgre_dbname = postgre_dbname
        self.postgre_user = postgre_user
        self.postgre_url = postgre_url
        self.postgre_pass = postgre_pass
        self.eu_db = eu_db
        self.dp_pop_db = dp_pop_db
        self.wwtp_con_db = wwtp_con_db
        self.comp_con_db = comp_con_db

    def __repr__(self):
        return self.config_name
