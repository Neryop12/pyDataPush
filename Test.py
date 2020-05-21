# diario_campanas(dfcampanas,db.FB['media'],conn)
dfcampanas = Spreadsheet(db.FB['key'], db.FB['media'], db.FB['campanas'])
dfadsets = Spreadsheet(db.FB['key'], db.FB['media'], db.FB['adsets'])
dfads = Spreadsheet(db.FB['key'], db.FB['media'], db.FB['ads'])


cuentas(dfcampanas, db.FB['media'], conn)
metricas_campanas(dfcampanas, db.FB['media'], conn)
campanas(dfcampanas, db.FB['media'], conn)
metricas_adsets(dfadsets, db.FB['media'], conn)
adsets(dfadsets, db.FB['media'], conn)
ads(dfads, db.FB['media'], conn)
metricas_ads(dfads, db.FB['media'], conn)
creative_ads(dfads, db.FB['media'], conn)

# Google

dfcampanas = Spreadsheet(db.GO['key'], db.GO['media'], db.GO['campanas'])
dfadsets = Spreadsheet(db.GO['key'], db.GO['media'], db.GO['adsets'])
dfads = Spreadsheet(db.GO['key'], db.GO['media'], db.GO['ads'])

# diario_campanas(dfcampanas,db.GO['media'],conn)

cuentas(dfcampanas, db.GO['media'], conn)
metricas_campanas(dfcampanas, db.GO['media'], conn)
campanas(dfcampanas, db.GO['media'], conn)
metricas_adsets(dfadsets, db.GO['media'], conn)
adsets(dfadsets, db.GO['media'], conn)
ads(dfads, db.GO['media'], conn)
metricas_ads(dfads, db.GO['media'], conn)


# Twitter

dfcampanas = Spreadsheet(db.TW['key'], db.TW['media'], db.TW['campanas'])
dfadsets = Spreadsheet(db.TW['key'], db.TW['media'], db.TW['adsets'])
dfads = Spreadsheet(db.TW['key'], db.TW['media'], db.TW['ads'])

# diario_campanas(dfcampanas,db.TW['media'],conn)

cuentas(dfcampanas, db.TW['media'], conn)
metricas_campanas(dfcampanas, db.TW['media'], conn)
campanas(dfcampanas, db.TW['media'], conn)
metricas_adsets(dfadsets, db.TW['media'], conn)
adsets(dfadsets, db.TW['media'], conn)
ads(dfads, db.TW['media'], conn)
metricas_ads(dfads, db.TW['media'], conn)


# Twitter

dfcampanas = Spreadsheet(db.TW['key'], db.TW['media'], db.TW['campanas'])
dfadsets = Spreadsheet(db.TW['key'], db.TW['media'], db.TW['adsets'])
dfads = Spreadsheet(db.TW['key'], db.TW['media'], db.TW['ads'])

# diario_campanas(dfcampanas,db.TW['media'],conn)

cuentas(dfcampanas, db.TW['media'], conn)
metricas_campanas(dfcampanas, db.TW['media'], conn)
campanas(dfcampanas, db.TW['media'], conn)
metricas_adsets(dfadsets, db.TW['media'], conn)
adsets(dfadsets, db.TW['media'], conn)
ads(dfads, db.TW['media'], conn)
metricas_ads(dfads, db.TW['media'], conn)
