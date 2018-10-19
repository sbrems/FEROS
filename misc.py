from astropy.time import Time


def find_night(dtime):
    dtime = Time(dtime, format='mjd')
    if (dtime.mjd % 1) >= 0.5:  # evening
        return dtime.iso[0:10].replace('-', '')
    else:  # morning. previous day
        return Time(dtime.jd - 1, format='jd').iso[0:10].replace('-', '')

