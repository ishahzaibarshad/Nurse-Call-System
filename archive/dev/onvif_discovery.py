from onvif import ONVIFCamera
from typing import Optional, Dict, List

class ONVIFCameraDiscovery:
    """Discovers camera RTSP URLs using ONVIF protocol"""
    
    def __init__(self, ip: str, port: int, username: str, password: str):
        self.ip = ip
        self.port = port
        self.username = username
        self.password = password
        self.camera = None
    
    def connect(self) -> bool:
        try:
            print(f"[ONVIF] Connecting to {self.ip}:{self.port}")
            self.camera = ONVIFCamera(self.ip, self.port, self.username, self.password)
            
            device_service = self.camera.create_devicemgmt_service()
            device_info = device_service.GetDeviceInformation()
            
            print(f"[ONVIF] Connected! {device_info.Manufacturer} {device_info.Model}")
            return True
            
        except Exception as e:
            print(f"[ONVIF] Failed to connect: {str(e)}")
            return False
    
    def get_rtsp_urls(self) -> List[Dict[str, str]]:
        if not self.camera:
            if not self.connect():
                return []
        
        try:
            media_service = self.camera.create_media_service()
            profiles = media_service.GetProfiles()
            
            rtsp_urls = []
            
            for profile in profiles:
                try:
                    stream_setup = {
                        'Stream': 'RTP-Unicast',
                        'Transport': {'Protocol': 'RTSP'}
                    }
                    
                    uri_response = media_service.GetStreamUri({
                        'StreamSetup': stream_setup,
                        'ProfileToken': profile.token
                    })
                    
                    rtsp_url = uri_response.Uri.replace('127.0.0.1', self.ip)
                    
                    if '@' not in rtsp_url and 'rtsp://' in rtsp_url:
                        rtsp_url = rtsp_url.replace('rtsp://', f'rtsp://{self.username}:{self.password}@')
                    
                    stream_info = {
                        'profile_name': profile.Name,
                        'profile_token': profile.token,
                        'rtsp_url': rtsp_url,
                        'resolution': f"{profile.VideoEncoderConfiguration.Resolution.Width}x{profile.VideoEncoderConfiguration.Resolution.Height}" if profile.VideoEncoderConfiguration else "Unknown"
                    }
                    
                    rtsp_urls.append(stream_info)
                    print(f"[ONVIF] Found: {stream_info['profile_name']} - {stream_info['rtsp_url']}")
                    
                except Exception as e:
                    print(f"[ONVIF] Error getting stream: {str(e)}")
                    continue
            
            return rtsp_urls
            
        except Exception as e:
            print(f"[ONVIF] Error: {str(e)}")
            return []
    
    def get_device_info(self) -> Optional[Dict]:
        if not self.camera:
            if not self.connect():
                return None
        
        try:
            device_service = self.camera.create_devicemgmt_service()
            device_info = device_service.GetDeviceInformation()
            
            return {
                'manufacturer': device_info.Manufacturer,
                'model': device_info.Model,
                'firmware': device_info.FirmwareVersion,
                'serial': device_info.SerialNumber,
                'hardware_id': device_info.HardwareId
            }
        except Exception as e:
            print(f"[ONVIF] Error: {str(e)}")
            return None
