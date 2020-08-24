import datetime
class PH():

    def last_day_of_month(self, date):
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month+1, day=1) - datetime.timedelta(days=1)
    
    def get_date_from_ph(self, iph, FYE_num):
        map_d   = {'H1':6, 'H2':0, 'Q1':9, 'Q2':6, 'Q3':3, 'H2':0, 'M9':3,'FY':0, 'Q4':0}
        def get_date_from_ph(ph):
            v   = FYE_num - map_d[ph[:-4]]
            ydiff   = 0
            #print (FYE_num, ph, map_d[ph[:-4]])
            if v < 0:
                v   = FYE_num + map_d[ph[:-4]]
                ydiff   = -1
            if v > 12 or v == 0:
                return ''
            pdate   = self.last_day_of_month(datetime.datetime.strptime('%s-%s-%s'%('01', v, int(ph[-4:]) +ydiff), '%d-%m-%Y')).strftime('%d-%b-%Y')
            return pdate
        return get_date_from_ph(iph)

    def get_ph_from_date(self, i_date, FYE_num, filing_type):
        fmap        = {
                            '10-Q':['Q1', 'Q2', 'Q3', 'Q4'],
                            '10-K':['FY'],
                        }
        year        = int(i_date.strftime('%Y'))
        d_ftm       = i_date.strftime('%d-%b-%Y')
        tmpi_date   = self.last_day_of_month(i_date)
        d_ftm       = tmpi_date.strftime('%d-%b-%Y')
        for ptype in fmap.get(filing_type, []):
            dstr    = self.get_date_from_ph('%s%s'%(ptype, year), FYE_num)
            if dstr == d_ftm:
                return ptype, year
        return '', ''
