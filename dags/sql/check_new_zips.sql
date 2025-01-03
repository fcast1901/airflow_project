SELECT 
    zc.zip, 
    zc.lat, 
    zc.lng
FROM 
    clima.zip_coordinates zc
LEFT JOIN 
    clima.check_zips cz
ON 
    zc.zip = cz.zip 
    AND zc.lat = cz.lat 
    AND zc.lng = cz.lng
WHERE 
    cz.zip IS NULL;
