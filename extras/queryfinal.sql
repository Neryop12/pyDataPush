select
a.Media as Medio,
c.idcampana as IDMFC,  
d.nombre as Campana,
d.nombreversion as Version,
c.multiplestiposg as Nomenclatura,
g.nombre as Objetivo
c.costo as Costo_planificado,
b.CampaingID as Plataforma_ID,
b.Campaingname as Plataforma_name,
b.StartDate as Fecha_inicio,
b.EndDate as Fecha_fin,
b.Cost as Costo_plataforma
from 
Accounts as a

inner join Campaings as b on a.AccountsID = b.AccountsID

inner join  mfcgt.mfccompradiaria c on c.multiplestiposg=b.CampaingIDMFC

inner join mfcgt.mfccampana d on d.id=c.idcampana

inner join mfcgt.dformatodigital e on e.id = d.idformatodigital

inner join mfcgt.danuncio f on f.id = e.idanuncio

inner join mfcgt.dmetrica g on g.id = f.idmetrica
