#%%
import xarray as xr

# 打开 NetCDF 文件
ds = xr.open_dataset(r"D:\GDROM\GDROM v2\gdromops\pdsi.mon.mean.nc")

print(ds)  # 看看文件里有啥维度/变量

#%%
import xarray as xr
import matplotlib.pyplot as plt

# 打开文件
ds = xr.open_dataset(r"D:\GDROM\GDROM v2\gdromops\pdsi.mon.mean.nc")

# 选择最近的格点
lat_point, lon_point = 35, -97
point_series = ds['pdsi'].sel(lat=lat_point, lon=lon_point, method="nearest")

# 转成 pandas.Series
pdsi_series = point_series.to_series()

print(pdsi_series.head(12))  # 前12个月

# 可视化
pdsi_series.plot(title=f"PDSI at lat={lat_point}, lon={lon_point}")
plt.show()
