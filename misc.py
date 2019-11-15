from astropy.time import Time


def find_night(dtime):
    dtime = Time(dtime, format='mjd')
    if (dtime.mjd % 1) >= 0.5:  # evening
        return Time(dtime.iso[:10], format='iso')
    else:  # morning. previous day
        return Time(Time((dtime.jd - 1), format='jd').iso[:10], format='iso')

