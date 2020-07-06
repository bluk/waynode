use crate::{error::Error, node::Id};
use rand::Rng;
use std::convert::TryFrom;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

// pub trait ToCompactAddress: ToSocketAddrs {
//     fn to_compact_address(&self) -> Result<Vec<u8>, Error>;
// }
//
// impl<T> ToCompactAddress for T
// where
//     T: ToSocketAddrs,
// {
//     fn to_compact_address(&self) -> Result<Vec<u8>, Error> {
//         let mut addrs = self.to_socket_addrs().map_err(|_| Error::InvalidNodeId)?;
//
//         match addrs.next().ok_or_else(|| Error::InvalidNodeId)? {
//             SocketAddr::V4(addr) => {
//                 let mut data = Vec::<u8>::new();
//                 data.extend_from_slice(&addr.ip().octets());
//                 data.extend_from_slice(&addr.port().to_be_bytes());
//                 Ok(data)
//             }
//             SocketAddr::V6(addr) => {
//                 let mut data = Vec::<u8>::new();
//                 data.extend_from_slice(&addr.ip().octets());
//                 data.extend_from_slice(&addr.port().to_be_bytes());
//                 Ok(data)
//             }
//         }
//     }
// }

pub struct CompactNodeInfo<T: CompactAddr> {
    pub id: Id,
    pub addr: T,
}

pub trait CompactAddr {}

pub trait CompactAddressV4: CompactAddr {
    fn to_compact_address(&self) -> [u8; 6];

    fn from_compact_address(bytes: [u8; 6]) -> Self;
}

impl CompactAddr for SocketAddrV4 {}

impl CompactAddressV4 for SocketAddrV4 {
    fn to_compact_address(&self) -> [u8; 6] {
        let mut a: [u8; 6] = [0; 6];
        a[0..4].copy_from_slice(&self.ip().octets());
        a[4..6].copy_from_slice(&self.port().to_be_bytes());
        a
    }

    fn from_compact_address(bytes: [u8; 6]) -> Self {
        let mut ip: [u8; 4] = [0; 4];
        ip[0..4].copy_from_slice(&bytes[0..4]);
        let ip = Ipv4Addr::from(ip);

        let mut port: [u8; 2] = [0; 2];
        port[0..2].copy_from_slice(&bytes[4..6]);
        let port = u16::from_be_bytes(port);

        SocketAddrV4::new(ip, port)
    }
}

pub trait CompactAddressV6: CompactAddr {
    fn to_compact_address(&self) -> [u8; 18];

    fn from_compact_address(bytes: [u8; 18]) -> Self;
}

impl CompactAddr for SocketAddrV6 {}

impl CompactAddressV6 for SocketAddrV6 {
    fn to_compact_address(&self) -> [u8; 18] {
        let mut a: [u8; 18] = [0; 18];
        a[0..16].copy_from_slice(&self.ip().octets());
        a[16..18].copy_from_slice(&self.port().to_be_bytes());
        a
    }

    fn from_compact_address(bytes: [u8; 18]) -> Self {
        let mut ip: [u8; 16] = [0; 16];
        ip[0..16].copy_from_slice(&bytes[0..16]);
        let ip = Ipv6Addr::from(ip);

        let mut port: [u8; 2] = [0; 2];
        port[0..2].copy_from_slice(&bytes[16..18]);
        let port = u16::from_be_bytes(port);

        SocketAddrV6::new(ip, port, 0, 0)
    }
}

trait Crc32cMaker {
    fn make_crc32c(&self, rand: u8) -> u32;
}

impl Crc32cMaker for Ipv4Addr {
    fn make_crc32c(&self, rand: u8) -> u32 {
        const MASK: [u8; 4] = [0x03, 0x0F, 0x3F, 0xFF];
        let r = rand & 0x7;

        let octets = self.octets();

        let mut masked_bytes: [u8; 4] = [0; 4];
        masked_bytes[0] = octets[0] & MASK[0];
        masked_bytes[1] = octets[1] & MASK[1];
        masked_bytes[2] = octets[2] & MASK[2];
        masked_bytes[3] = octets[3] & MASK[3];

        masked_bytes[0] |= r << 5;

        crc32c::crc32c(&masked_bytes)
    }
}

impl Crc32cMaker for Ipv6Addr {
    fn make_crc32c(&self, rand: u8) -> u32 {
        const MASK: [u8; 8] = [0x01, 0x03, 0x07, 0x0F, 0x01F, 0x3F, 0x7F, 0xFF];
        let r = rand & 0x7;

        let octets = self.octets();

        let mut masked_bytes: [u8; 8] = [0; 8];
        masked_bytes[0] = octets[0] & MASK[0];
        masked_bytes[1] = octets[1] & MASK[1];
        masked_bytes[2] = octets[2] & MASK[2];
        masked_bytes[3] = octets[3] & MASK[3];
        masked_bytes[4] = octets[4] & MASK[4];
        masked_bytes[5] = octets[5] & MASK[5];
        masked_bytes[6] = octets[6] & MASK[6];
        masked_bytes[7] = octets[7] & MASK[7];

        masked_bytes[0] |= r << 5;

        crc32c::crc32c(&masked_bytes)
    }
}

pub trait NodeIdGenerator {
    fn make_node_id(&self, rand: Option<u8>) -> Result<Id, Error>;

    fn is_valid_node_id(&self, id: &Id) -> bool;
}

impl NodeIdGenerator for Ipv4Addr {
    fn make_node_id(&self, rand: Option<u8>) -> Result<Id, Error> {
        let rand = rand.unwrap_or_else(|| rand::thread_rng().gen_range(0, 8));
        let crc32_val = self.make_crc32c(rand);
        let mut id = Id::rand()?;
        id.0[0] = u8::try_from(crc32_val >> 24 & 0xFF).unwrap();
        id.0[1] = u8::try_from(crc32_val >> 16 & 0xFF).unwrap();
        id.0[2] = u8::try_from(crc32_val >> 8 & 0xF8 | rand::thread_rng().gen_range(0, 8)).unwrap();
        id.0[19] = rand;
        Ok(id)
    }

    fn is_valid_node_id(&self, id: &Id) -> bool {
        let octets = self.octets();
        // loopback
        if octets[0] == 127 {
            return true;
        }

        // self-assigned
        if octets[0] == 169 && octets[1] == 254 {
            return true;
        }

        // local network
        if octets[0] == 10
            || (octets[0] == 172 && octets[1] >> 4 == 1)
            || (octets[0] == 192 && octets[1] == 168)
        {
            return true;
        }

        let rand = id.0[19];
        let crc32c_val = self.make_crc32c(rand);

        dbg!(crc32c_val);

        if id.0[0] != u8::try_from((crc32c_val >> 24) & 0xFF).unwrap() {
            return false;
        }

        if id.0[1] != u8::try_from((crc32c_val >> 16) & 0xFF).unwrap() {
            return false;
        }

        if (id.0[2] & 0xF8) != u8::try_from((crc32c_val >> 8) & 0xF8).unwrap() {
            return false;
        }

        true
    }
}

impl NodeIdGenerator for Ipv6Addr {
    fn make_node_id(&self, rand: Option<u8>) -> Result<Id, Error> {
        let rand = rand.unwrap_or_else(|| rand::thread_rng().gen_range(0, 8));
        let crc32_val = self.make_crc32c(rand);
        let mut id = Id::rand()?;
        id.0[0] = u8::try_from(crc32_val >> 24 & 0xFF).unwrap();
        id.0[1] = u8::try_from(crc32_val >> 16 & 0xFF).unwrap();
        id.0[2] = u8::try_from(crc32_val >> 8 & 0xF8 | rand::thread_rng().gen_range(0, 8)).unwrap();
        id.0[19] = rand;
        Ok(id)
    }

    fn is_valid_node_id(&self, id: &Id) -> bool {
        let rand = id.0[19];
        let crc32c_val = self.make_crc32c(rand);

        if id.0[0] != u8::try_from((crc32c_val >> 24) & 0xFF).unwrap() {
            return false;
        }

        if id.0[1] != u8::try_from((crc32c_val >> 16) & 0xFF).unwrap() {
            return false;
        }

        if (id.0[2] & 0xF8) != u8::try_from((crc32c_val >> 8) & 0xF8).unwrap() {
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipv4_make_node_id_1() {
        let ip = "124.31.75.21".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(&id));
    }

    #[test]
    fn test_ipv4_valid_node_id_1() {
        let ip = "124.31.75.21".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(&Id::new_with_bytes([
            0x5f, 0xbf, 0xbf, 0xf1, 0x0c, 0x5d, 0x6a, 0x4e, 0xc8, 0xa8, 0x8e, 0x4c, 0x6a, 0xb4,
            0xc2, 0x8b, 0x95, 0xee, 0xe4, 0x01
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_2() {
        let ip = "21.75.31.124".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(&id));
    }

    #[test]
    fn test_ipv4_valid_node_id_2() {
        let ip = "21.75.31.124".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(&Id::new_with_bytes([
            0x5a, 0x3c, 0xe9, 0xc1, 0x4e, 0x7a, 0x08, 0x64, 0x56, 0x77, 0xbb, 0xd1, 0xcf, 0xe7,
            0xd8, 0xf9, 0x56, 0xd5, 0x32, 0x56
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_3() {
        let ip = "65.23.51.170".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(&id));
    }

    #[test]
    fn test_ipv4_valid_node_id_3() {
        let ip = "65.23.51.170".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(&Id::new_with_bytes([
            0xa5, 0xd4, 0x32, 0x20, 0xbc, 0x8f, 0x11, 0x2a, 0x3d, 0x42, 0x6c, 0x84, 0x76, 0x4f,
            0x8c, 0x2a, 0x11, 0x50, 0xe6, 0x16
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_4() {
        let ip = "84.124.73.14".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(&id));
    }

    #[test]
    fn test_ipv4_valid_node_id_4() {
        let ip = "84.124.73.14".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(&Id::new_with_bytes([
            0x1b, 0x03, 0x21, 0xdd, 0x1b, 0xb1, 0xfe, 0x51, 0x81, 0x01, 0xce, 0xef, 0x99, 0x46,
            0x2b, 0x94, 0x7a, 0x01, 0xff, 0x41
        ])));
    }

    #[test]
    fn test_ipv4_make_node_id_5() {
        let ip = "43.213.53.83".parse::<Ipv4Addr>().unwrap();
        let id = ip.make_node_id(None).unwrap();
        assert!(ip.is_valid_node_id(&id));
    }

    #[test]
    fn test_ipv4_valid_node_id_5() {
        let ip = "43.213.53.83".parse::<Ipv4Addr>().unwrap();
        assert!(ip.is_valid_node_id(&Id::new_with_bytes([
            0xe5, 0x6f, 0x6c, 0xbf, 0x5b, 0x7c, 0x4b, 0xe0, 0x23, 0x79, 0x86, 0xd5, 0x24, 0x3b,
            0x87, 0xaa, 0x6d, 0x51, 0x30, 0x5a
        ])));
    }
}
