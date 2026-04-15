- Replace: `https://www.gridprotectionalliance.org`  to `https://at-energy.vn`
- Replace: `2-Line - 500.png` to `ATDigital_w500.png`
- Delete: `companylogo.jpg` , `companylogo2.jpg`, `TVAcompanylogo.jpg`
- Replace: `EPRI(c).jpg` to `ATEnergy_h33.png`
- Replace: `GPA-Logo.png` to `ATEnergy_h33.png`
- Replace: `GPA-Logo---30-pix(on-white).png` to `ATDigital_h33.png`
- Replace: `openSEE - Waveform Viewer Header.png` to `WaveformViewer_h30.png`



- Replace: `Open PQ Dashboard` to `PQ Dashboard`
- Replace: `Open PQDashboard` to `PQ Dashboard`
- Replace: `Grid Protection Alliance` to `Company Name`
- Replace: `openXDA ` to `ATDigital Disturbance Analytics `
- Replace: `View in OpenXDA` to `View in ATDigital Disturbance Analytics`
- Replace: `Launch OpenSEE Waveform Viewer` to `Launch Waveform Viewer`
- Replace: `Launch OpenSTE Trending Viewer` to `Launch Trending Viewer`
- Replace: `OpenSTE System Trending Explorer` to `System Trending Explorer`

- Scrips\OpenSEE.js (line 21616) & Scrips\PQDashboard.js (line 24538): Remove `return (...` to `return (null)`;  //Xu ly duong dan : https://github.com/GridProtectionAlliance/openXDA
- Settings.cshtml (line 224): `<div id="about" ...`