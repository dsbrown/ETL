# Global Settings
db_name = "log" # both name of RDS server and mysql database
db_size = 5 #GB
db_machine = 'db.m1.small'
db_user=""
db_password = "notarealpassword"
db_instance= "mysql"
db_name = "the_db"
db_sg_name = 'log'
db_security_group = "web_servers"
#db_endpoint = "pdxmysql.cld9sdiplb4g.us-west-2.rds.com:3306"
ec2_key_name = 'log'
ec2_security_group_name = 'log'
ec2_instance_type = 't1.micro'
ec2_ami = '?????'


SiteTbl_desc={
  'SiteId_tbl' : 'INT',
  'ClusterName' : 'VARCHAR',
  'ClusterID' : 'VARCHAR',
  'ProjectName' : 'VARCHAR',
  'AWSSiteCode' : 'VARCHAR',
  'VendorName' : 'VARCHAR',
  'VendorSiteName' : 'VARCHAR',
  'VendorSiteAddress' : 'VARCHAR',
  'VendorSiteCity' : 'VARCHAR',
  'VendorSiteState' : 'VARCHAR',
  'VendorSiteCountry' : 'VARCHAR',
  'VendorSiteLatitude' : 'DOUBLE',
  'VendorSiteLongitude' : 'DOUBLE',
  'SelectedByBiz' : 'DATETIME',
  'InProd' : 'INT',
  'InDev' : 'INT',
  'TypeEC2' : 'INT',
  'TypeCritNet' : 'INT',
  'TypeHPC' : 'INT',
  'TypeVPC' : 'INT',
  'TypeProd' : 'INT',
  'TypeCorp' : 'INT',
  'TypeDX' : 'INT',
  'TypeAIV_CF' : 'INT',
  'TypeTCF_CF' : 'INT',
  'TypeGrumpy' : 'INT',
  'Type_X1' : 'INT',
  'Type_X2' : 'INT',
  'Type_X3' : 'INT',
  'Type_X4' : 'INT',
  'TypeOther' : 'INT',
  'LSEvents' : 'INT',
  'SiteUUID' : 'VARCHAR',
}



AssessmentResultsTbl_desc={ 
  'SiteId' : 'INT',
  'idAssessmentTable' : 'INT',
  'VendorSiteVisitDate' :  'DATETIME',
  'Engineer' : 'VARCHAR',
  'BizDevRepresentative' : 'VARCHAR',
  'Ph1ScoreTotal': 'FLOAT',
  'Ph1ScoreAIA02': 'FLOAT',
  'Ph1ScoreAIA11': 'FLOAT',
  'Ph1ScoreAIA21': 'FLOAT',
  'Ph1ScoreAIA23': 'FLOAT',
  'Ph1ScoreAIA25': 'FLOAT',
  'Ph1ScoreAIA26': 'FLOAT',
  'Ph1ScoreAIA27': 'FLOAT',
  'Ph1ScoreAIA28': 'FLOAT',
  'Ph1ScoreAIA33': 'FLOAT',
  'Ph2ScoreTotal': 'FLOAT',
  'Ph2ScoreAIA02': 'FLOAT',
  'Ph2ScoreAIA11': 'FLOAT',
  'Ph2ScoreAIA21': 'FLOAT',
  'Ph2ScoreAIA23': 'FLOAT',
  'Ph2ScoreAIA25': 'FLOAT',
  'Ph2ScoreAIA26': 'FLOAT',
  'Ph2ScoreAIA27': 'FLOAT',
  'Ph2ScoreAIA28': 'FLOAT',
  'Ph2ScoreAIA33': 'FLOAT',
  'Ph3ScoreTotal': 'FLOAT',
  'Ph3ScoreAIA02': 'FLOAT',
  'Ph3ScoreAIA11': 'FLOAT',
  'Ph3ScoreAIA21': 'FLOAT',
  'Ph3ScoreAIA23': 'FLOAT',
  'Ph3ScoreAIA25': 'FLOAT',
  'Ph3ScoreAIA26': 'FLOAT',
  'Ph3ScoreAIA27': 'FLOAT',
  'Ph3ScoreAIA28': 'FLOAT',
  'Ph3ScoreAIA33': 'FLOAT',
  'CompScoreAIA02': 'FLOAT',
  'CompScoreAIA11': 'FLOAT',
  'CompScoreAIA21': 'FLOAT',
  'CompScoreAIA23': 'FLOAT',
  'CompScoreAIA25': 'FLOAT',
  'CompScoreAIA26': 'FLOAT',
  'CompScoreAIA27': 'FLOAT',
  'CompScoreAIA28': 'FLOAT',
  'CompScoreAIA33': 'FLOAT',
  'CompositeScore': 'FLOAT',
  'ReasonFor' : 'TEXT',
  'ReasonAgainst' : 'TEXT',
  'ShowStoppers' : 'TEXT',
  'PrelimReportDate' : 'DATETIME',
  'FinalReportDate' : 'DATETIME',
  'SiteDeliverDate' : 'DATETIME',
  'Select' : 'INT',
  'BizDevSelect' : 'INT',
  'AssessmentGUID' : 'VARCHAR',
  'SiteUUID' : 'INT',
}


AuditResultsTbl_desc = {
  'idAuditTbl' : 'INT',
  'AuditDate' : 'DATETIME',
  'AuditFnlRpt' : 'DATETIME',
  'AuditPrelimRpt' : 'DATETIME',
  'C_SPOFS' : 'INT',
  'C_Risks' : 'INT',
  'SPOFS' : 'INT',
  'Risks' : 'INT',
  'PreviousAI' : 'INT',
  'NewAI' : 'INT',
  'AgreedAI' : 'INT',
  'RemediedAI' : 'INT',
  'SiteId' : 'INT',
  'SiteUUID' : 'VARCHAR',
  'AuditResults' : 'TEXT',
  'AuditActions' : 'TEXT',
}

#This dictionary hash is used by the logging module
LOGGING = {
  'version': 1,
  'disable_existing_loggers': True,
  'formatters': {
    'default': {
      'format': '%(levelname)s %(asctime)s %(message)s',
      'datefmt': '%m/%d/%Y %H:%M%S',
    }
  },
  'handlers': {
    'null': {
      'level': 'DEBUG',
      'class': 'logging.NullHandler',
    },
    'console': {
      'level': 'DEBUG',
      'class': 'logging.StreamHandler',
      'formatter': 'default',
    },
    'file': {
      'level': 'INFO',
      'class': 'logging.handlers.TimedRotatingFileHandler',
      'formatter': 'default',
      'filename': 'temp',
      'when': 'M',
      'interval': 1,
    }
  },
  'Loggers': {
    'EndMemo': {
      'level': 'INFO',
      'handlers': ['console','file'],
      'propagate': True,
    }
  }
}

