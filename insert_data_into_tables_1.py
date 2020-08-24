import os;
import sys;
import MySQLdb;

def db_link():
    conn = MySQLdb.connect("172.16.20.229", "root", "tas123", "project_company_mgmt_db_test")
    cur = conn.cursor()
    return conn , cur


def insert_into_tables():
    conn , cur = db_link()
    print   conn , cur
    #monthj_lists =  ['','January','February','March','April','May','June','July','August','September','October','November','December']
    #language =[['',''],['Afrikaans', 'af'], ['Albanian', 'sq'], ['Arabic', 'ar'], ['Azerbaijani', 'az'], ['Basque', 'eu'], ['Bengali', 'bn'], ['Belarusian', 'be'], ['Bulgarian', 'bg'], ['Catalan', 'ca'], ['Chinese Simplified', 'zh-CN'], ['Chinese Traditional', 'zh-TW'], ['Croatian', 'hr'], ['Czech', 'cs'], ['Danish', 'da'], ['Dutch', 'nl'], ['English', 'en'], ['Esperanto', 'eo'], ['Estonian', 'et'], ['Filipino', 'tl'], ['Finnish', 'fi'], ['French', 'fr'], ['Galician', 'gl'], ['Georgian', 'ka'], ['German', 'de'], ['Greek', 'el'], ['Gujarati', 'gu'], ['Haitian Creole', 'ht'], ['Hebrew', 'iw'], ['Hindi', 'hi'], ['Hungarian', 'hu'], ['Icelandic', 'is'], ['Indonesian', 'id'], ['Irish', 'ga'], ['Italian', 'it'], ['Japanese', 'ja'], ['Kannada', 'kn'], ['Korean', 'ko'], ['Latin', 'la'], ['Latvian', 'lv'], ['Lithuanian', 'lt'], ['Macedonian', 'mk'], ['Malay', 'ms'], ['Maltese', 'mt'], ['Norwegian', 'no'], ['Persian', 'fa'], ['Polish', 'pl'], ['Portuguese', 'pt'], ['Romanian', 'ro'], ['Russian', 'ru'], ['Serbian', 'sr'], ['Slovak', 'sk'], ['Slovenian', 'sl'], ['Spanish', 'es'], ['Swahili', 'sw'], ['Swedish', 'sv'], ['Tamil', 'ta'], ['Telugu', 'te'], ['Thai', 'th'], ['Turkish', 'tr'], ['Ukrainian', 'uk'], ['Urdu', 'ur'], ['Vietnamese', 'vi'], ['Welsh', 'cy'], ['Yiddish', 'yi']]
    access_standard = ['','IFRS','US-GAAP' ]
    #filing_frequency = ['','Q1','Q2','Q3','FY', 'Q4','H1','H2', 'M9']
    #industry = ['','Aerospace & Defense','Asset Management','Automobiles','Banks','Beverages','Biotechnology','Business Services','Chemicals','Commercial Airlines','Communications','Computers & Software','Construction & Engineering','Consumer Finance','Diversified Financial Services','Education','Energy','Finance / Banking / Investment','Food Products','Health Care Providers & Services','Hedge Funds','Hotels, Restaurants & Leisure','Industrial Conglomerates','Insurance','Internet','IT Services','Legal Services','Leisure Equipment & Products','Manufacturing','Media','Metals & Mining','Non-Profit & Social Organizations','Oil, Gas & Consumable Fuels','Paper & Forest Products','Pharmaceuticals','Professional Services / Accounting / Consulting','Real Estate','Real Estate Investment Trusts','Real Estate Management & Development','Retail','Road & Rail','Semiconductors & Semiconductor Equipment','Shipping / Packaging / Distribution','Sports & Entertainment','Textiles, Apparel & Luxury Goods','Utilities (Electric, Water, Gas)','Venture Capital']
    #for mon, val in curr_lst.items():
    country =['','Afghanistan','Albania','Algeria','Andorra','Angola','Antigua & Deps','Argentina','Armenia','Australia','Austria','Azerbaijan','Bahamas','Bahrain','Bangladesh','Barbados','Belarus','Belgium','Belize','Benin','Bhutan','Bolivia','Bosnia Herzegovina','Botswana','Brazil','Brunei','Bulgaria','Burkina','Burundi','Cambodia','Cameroon','Canada','Cape Verde','Central African Rep','Chad','Chile','China','Colombia','Comoros','Congo','Congo {Democratic Rep}','Costa Rica','Croatia','Cuba','Cyprus','Czech Republic','Denmark','Djibouti','Dominica','Dominican Republic','East Timor','Ecuador','Egypt','El Salvador','Equatorial Guinea','Eritrea','Estonia','Ethiopia','Fiji','Finland','France','Gabon','Gambia','Georgia','Germany','Ghana','Greece','Grenada','Guatemala','Guinea','Guinea-Bissau','Guyana','Haiti','Honduras','Hungary','Iceland','India','Indonesia','Iran','Iraq','Ireland {Republic}','Israel','Italy','Ivory Coast','Jamaica','Japan','Jordan','Kazakhstan','Kenya','Kiribati','Korea North','Korea South','Kosovo','Kuwait','Kyrgyzstan','Laos','Latvia','Lebanon','Lesotho','Liberia','Libya','Liechtenstein','Lithuania','Luxembourg','Macedonia','Madagascar','Malawi','Malaysia','Maldives','Mali','Malta','Marshall Islands','Mauritania','Mauritius','Mexico','Micronesia','Moldova','Monaco','Mongolia','Montenegro','Morocco','Mozambique','Myanmar, {Burma}','Namibia','Nauru','Nepal','Netherlands','New Zealand','Nicaragua','Niger','Nigeria','Norway','Oman','Pakistan','Palau','Panama','Papua New Guinea','Paraguay','Peru','Philippines','Poland','Portugal','Qatar','Romania','Russian Federation','Rwanda','St Kitts & Nevis','St Lucia','Saint Vincent & the Grenadines','Samoa','San Marino','Sao Tome & Principe','Saudi Arabia','Senegal','Serbia','Seychelles','Sierra Leone','Singapore','Slovakia','Slovenia','Solomon Islands','Somalia','South Africa','South Sudan','Spain','Sri Lanka','Sudan','Suriname','Swaziland','Sweden','Switzerland','Syria','Taiwan','Tajikistan','Tanzania','Thailand','Togo','Tonga','Trinidad & Tobago','Tunisia','Turkey','Turkmenistan','Tuvalu','Uganda','Ukraine','United Arab Emirates','United Kingdom','United States','Uruguay','Uzbekistan','Vanuatu','Vatican City','Venezuela','Vietnam','Yemen','Zambia','Zimbabwe']
    for mon in country:
        print mon
        #"""
        #sql_in = "insert into Languages(language, code) values ('%s', '%s')"%(str(mon[0]), str(mon[1]))
        #sql_in = "insert into Months(month) values ('%s')"%(mon)
        #sql_in = "insert into Accounting_Standard(accounting_standard) values ('%s')"%(mon)
        sql_in = "insert into country(country) values ('%s')"%(mon)
        #sql_in = """insert into industrytype(industryName) values ("%s")"""%(mon)
        print sql_in
        cur.execute(sql_in)
        conn.commit() 
        """
        sql_in = "insert into Currency(currency, code) values("%s", "%s")"%(str(mon), str(val))
        print sql_in
        cur.execute(sql_in)
        """


if __name__ == '__main__':
   insert_into_tables()
