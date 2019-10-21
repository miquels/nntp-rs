/// Diablo CRC64 hash.
#[derive(Clone, Debug, Default, PartialEq)]
#[repr(C)]
pub struct DHash {
    pub h1: u32,
    pub h2: u32,
}

// The table below was generated by running this code:
//
// const CRC_POLY1 : u32 = 0x00600340;
// const CRC_POLY2 : u32 = 0x00F0D50B;
//
// fn crc_init() -> Vec<DHash> {
//     let mut table = Vec::with_capacity(256);
//     for i in 0..256 {
//         let mut v = i as u32;
//         let mut hv = DHash{h1: 0, h2: 0};
//
//         for _ in 0..8 {
//             if (v & 0x80) != 0 {
//                 hv.h1 ^= CRC_POLY1;
//                 hv.h2 ^= CRC_POLY2;
//             }
//             hv.h2 = hv.h2 << 1;
//             if (hv.h1 & 0x80000000) != 0 {
//                 hv.h2 |= 1;
//             }
//             hv.h1 <<= 1;
//             v <<= 1;
//         }
//         table.push(hv);
//     }
//     table
// }
//
// fn main() {
//     let t = crc_init();
//     let mut i = 0;
//     println!("static CRC_XOR_TABLE : [DHash; 256] = [");
//     while i < 256 {
//         print!("  DHash{{ h1: 0x{:08x}, h2: 0x{:08x} }}, ", t[i].h1, t[i].h2);
//         println!("DHash{{ h1: 0x{:08x}, h2: 0x{:08x} }},", t[i+1].h1, t[i+1].h2);
//         i += 2;
//     }
//     println!("];");
// }

#[rustfmt::skip]
static CRC_XOR_TABLE : [DHash; 256] = [
  DHash{ h1: 0x00000000, h2: 0x00000000 }, DHash{ h1: 0x00c00680, h2: 0x01e1aa16 },
  DHash{ h1: 0x01800d00, h2: 0x03c3542c }, DHash{ h1: 0x01400b80, h2: 0x0222fe3a },
  DHash{ h1: 0x03001a00, h2: 0x0786a858 }, DHash{ h1: 0x03c01c80, h2: 0x0667024e },
  DHash{ h1: 0x02801700, h2: 0x0445fc74 }, DHash{ h1: 0x02401180, h2: 0x05a45662 },
  DHash{ h1: 0x06003400, h2: 0x0f0d50b0 }, DHash{ h1: 0x06c03280, h2: 0x0eecfaa6 },
  DHash{ h1: 0x07803900, h2: 0x0cce049c }, DHash{ h1: 0x07403f80, h2: 0x0d2fae8a },
  DHash{ h1: 0x05002e00, h2: 0x088bf8e8 }, DHash{ h1: 0x05c02880, h2: 0x096a52fe },
  DHash{ h1: 0x04802300, h2: 0x0b48acc4 }, DHash{ h1: 0x04402580, h2: 0x0aa906d2 },
  DHash{ h1: 0x0c006800, h2: 0x1e1aa160 }, DHash{ h1: 0x0cc06e80, h2: 0x1ffb0b76 },
  DHash{ h1: 0x0d806500, h2: 0x1dd9f54c }, DHash{ h1: 0x0d406380, h2: 0x1c385f5a },
  DHash{ h1: 0x0f007200, h2: 0x199c0938 }, DHash{ h1: 0x0fc07480, h2: 0x187da32e },
  DHash{ h1: 0x0e807f00, h2: 0x1a5f5d14 }, DHash{ h1: 0x0e407980, h2: 0x1bbef702 },
  DHash{ h1: 0x0a005c00, h2: 0x1117f1d0 }, DHash{ h1: 0x0ac05a80, h2: 0x10f65bc6 },
  DHash{ h1: 0x0b805100, h2: 0x12d4a5fc }, DHash{ h1: 0x0b405780, h2: 0x13350fea },
  DHash{ h1: 0x09004600, h2: 0x16915988 }, DHash{ h1: 0x09c04080, h2: 0x1770f39e },
  DHash{ h1: 0x08804b00, h2: 0x15520da4 }, DHash{ h1: 0x08404d80, h2: 0x14b3a7b2 },
  DHash{ h1: 0x1800d000, h2: 0x3c3542c0 }, DHash{ h1: 0x18c0d680, h2: 0x3dd4e8d6 },
  DHash{ h1: 0x1980dd00, h2: 0x3ff616ec }, DHash{ h1: 0x1940db80, h2: 0x3e17bcfa },
  DHash{ h1: 0x1b00ca00, h2: 0x3bb3ea98 }, DHash{ h1: 0x1bc0cc80, h2: 0x3a52408e },
  DHash{ h1: 0x1a80c700, h2: 0x3870beb4 }, DHash{ h1: 0x1a40c180, h2: 0x399114a2 },
  DHash{ h1: 0x1e00e400, h2: 0x33381270 }, DHash{ h1: 0x1ec0e280, h2: 0x32d9b866 },
  DHash{ h1: 0x1f80e900, h2: 0x30fb465c }, DHash{ h1: 0x1f40ef80, h2: 0x311aec4a },
  DHash{ h1: 0x1d00fe00, h2: 0x34beba28 }, DHash{ h1: 0x1dc0f880, h2: 0x355f103e },
  DHash{ h1: 0x1c80f300, h2: 0x377dee04 }, DHash{ h1: 0x1c40f580, h2: 0x369c4412 },
  DHash{ h1: 0x1400b800, h2: 0x222fe3a0 }, DHash{ h1: 0x14c0be80, h2: 0x23ce49b6 },
  DHash{ h1: 0x1580b500, h2: 0x21ecb78c }, DHash{ h1: 0x1540b380, h2: 0x200d1d9a },
  DHash{ h1: 0x1700a200, h2: 0x25a94bf8 }, DHash{ h1: 0x17c0a480, h2: 0x2448e1ee },
  DHash{ h1: 0x1680af00, h2: 0x266a1fd4 }, DHash{ h1: 0x1640a980, h2: 0x278bb5c2 },
  DHash{ h1: 0x12008c00, h2: 0x2d22b310 }, DHash{ h1: 0x12c08a80, h2: 0x2cc31906 },
  DHash{ h1: 0x13808100, h2: 0x2ee1e73c }, DHash{ h1: 0x13408780, h2: 0x2f004d2a },
  DHash{ h1: 0x11009600, h2: 0x2aa41b48 }, DHash{ h1: 0x11c09080, h2: 0x2b45b15e },
  DHash{ h1: 0x10809b00, h2: 0x29674f64 }, DHash{ h1: 0x10409d80, h2: 0x2886e572 },
  DHash{ h1: 0x3001a000, h2: 0x786a8580 }, DHash{ h1: 0x30c1a680, h2: 0x798b2f96 },
  DHash{ h1: 0x3181ad00, h2: 0x7ba9d1ac }, DHash{ h1: 0x3141ab80, h2: 0x7a487bba },
  DHash{ h1: 0x3301ba00, h2: 0x7fec2dd8 }, DHash{ h1: 0x33c1bc80, h2: 0x7e0d87ce },
  DHash{ h1: 0x3281b700, h2: 0x7c2f79f4 }, DHash{ h1: 0x3241b180, h2: 0x7dced3e2 },
  DHash{ h1: 0x36019400, h2: 0x7767d530 }, DHash{ h1: 0x36c19280, h2: 0x76867f26 },
  DHash{ h1: 0x37819900, h2: 0x74a4811c }, DHash{ h1: 0x37419f80, h2: 0x75452b0a },
  DHash{ h1: 0x35018e00, h2: 0x70e17d68 }, DHash{ h1: 0x35c18880, h2: 0x7100d77e },
  DHash{ h1: 0x34818300, h2: 0x73222944 }, DHash{ h1: 0x34418580, h2: 0x72c38352 },
  DHash{ h1: 0x3c01c800, h2: 0x667024e0 }, DHash{ h1: 0x3cc1ce80, h2: 0x67918ef6 },
  DHash{ h1: 0x3d81c500, h2: 0x65b370cc }, DHash{ h1: 0x3d41c380, h2: 0x6452dada },
  DHash{ h1: 0x3f01d200, h2: 0x61f68cb8 }, DHash{ h1: 0x3fc1d480, h2: 0x601726ae },
  DHash{ h1: 0x3e81df00, h2: 0x6235d894 }, DHash{ h1: 0x3e41d980, h2: 0x63d47282 },
  DHash{ h1: 0x3a01fc00, h2: 0x697d7450 }, DHash{ h1: 0x3ac1fa80, h2: 0x689cde46 },
  DHash{ h1: 0x3b81f100, h2: 0x6abe207c }, DHash{ h1: 0x3b41f780, h2: 0x6b5f8a6a },
  DHash{ h1: 0x3901e600, h2: 0x6efbdc08 }, DHash{ h1: 0x39c1e080, h2: 0x6f1a761e },
  DHash{ h1: 0x3881eb00, h2: 0x6d388824 }, DHash{ h1: 0x3841ed80, h2: 0x6cd92232 },
  DHash{ h1: 0x28017000, h2: 0x445fc740 }, DHash{ h1: 0x28c17680, h2: 0x45be6d56 },
  DHash{ h1: 0x29817d00, h2: 0x479c936c }, DHash{ h1: 0x29417b80, h2: 0x467d397a },
  DHash{ h1: 0x2b016a00, h2: 0x43d96f18 }, DHash{ h1: 0x2bc16c80, h2: 0x4238c50e },
  DHash{ h1: 0x2a816700, h2: 0x401a3b34 }, DHash{ h1: 0x2a416180, h2: 0x41fb9122 },
  DHash{ h1: 0x2e014400, h2: 0x4b5297f0 }, DHash{ h1: 0x2ec14280, h2: 0x4ab33de6 },
  DHash{ h1: 0x2f814900, h2: 0x4891c3dc }, DHash{ h1: 0x2f414f80, h2: 0x497069ca },
  DHash{ h1: 0x2d015e00, h2: 0x4cd43fa8 }, DHash{ h1: 0x2dc15880, h2: 0x4d3595be },
  DHash{ h1: 0x2c815300, h2: 0x4f176b84 }, DHash{ h1: 0x2c415580, h2: 0x4ef6c192 },
  DHash{ h1: 0x24011800, h2: 0x5a456620 }, DHash{ h1: 0x24c11e80, h2: 0x5ba4cc36 },
  DHash{ h1: 0x25811500, h2: 0x5986320c }, DHash{ h1: 0x25411380, h2: 0x5867981a },
  DHash{ h1: 0x27010200, h2: 0x5dc3ce78 }, DHash{ h1: 0x27c10480, h2: 0x5c22646e },
  DHash{ h1: 0x26810f00, h2: 0x5e009a54 }, DHash{ h1: 0x26410980, h2: 0x5fe13042 },
  DHash{ h1: 0x22012c00, h2: 0x55483690 }, DHash{ h1: 0x22c12a80, h2: 0x54a99c86 },
  DHash{ h1: 0x23812100, h2: 0x568b62bc }, DHash{ h1: 0x23412780, h2: 0x576ac8aa },
  DHash{ h1: 0x21013600, h2: 0x52ce9ec8 }, DHash{ h1: 0x21c13080, h2: 0x532f34de },
  DHash{ h1: 0x20813b00, h2: 0x510dcae4 }, DHash{ h1: 0x20413d80, h2: 0x50ec60f2 },
  DHash{ h1: 0x60034000, h2: 0xf0d50b00 }, DHash{ h1: 0x60c34680, h2: 0xf134a116 },
  DHash{ h1: 0x61834d00, h2: 0xf3165f2c }, DHash{ h1: 0x61434b80, h2: 0xf2f7f53a },
  DHash{ h1: 0x63035a00, h2: 0xf753a358 }, DHash{ h1: 0x63c35c80, h2: 0xf6b2094e },
  DHash{ h1: 0x62835700, h2: 0xf490f774 }, DHash{ h1: 0x62435180, h2: 0xf5715d62 },
  DHash{ h1: 0x66037400, h2: 0xffd85bb0 }, DHash{ h1: 0x66c37280, h2: 0xfe39f1a6 },
  DHash{ h1: 0x67837900, h2: 0xfc1b0f9c }, DHash{ h1: 0x67437f80, h2: 0xfdfaa58a },
  DHash{ h1: 0x65036e00, h2: 0xf85ef3e8 }, DHash{ h1: 0x65c36880, h2: 0xf9bf59fe },
  DHash{ h1: 0x64836300, h2: 0xfb9da7c4 }, DHash{ h1: 0x64436580, h2: 0xfa7c0dd2 },
  DHash{ h1: 0x6c032800, h2: 0xeecfaa60 }, DHash{ h1: 0x6cc32e80, h2: 0xef2e0076 },
  DHash{ h1: 0x6d832500, h2: 0xed0cfe4c }, DHash{ h1: 0x6d432380, h2: 0xeced545a },
  DHash{ h1: 0x6f033200, h2: 0xe9490238 }, DHash{ h1: 0x6fc33480, h2: 0xe8a8a82e },
  DHash{ h1: 0x6e833f00, h2: 0xea8a5614 }, DHash{ h1: 0x6e433980, h2: 0xeb6bfc02 },
  DHash{ h1: 0x6a031c00, h2: 0xe1c2fad0 }, DHash{ h1: 0x6ac31a80, h2: 0xe02350c6 },
  DHash{ h1: 0x6b831100, h2: 0xe201aefc }, DHash{ h1: 0x6b431780, h2: 0xe3e004ea },
  DHash{ h1: 0x69030600, h2: 0xe6445288 }, DHash{ h1: 0x69c30080, h2: 0xe7a5f89e },
  DHash{ h1: 0x68830b00, h2: 0xe58706a4 }, DHash{ h1: 0x68430d80, h2: 0xe466acb2 },
  DHash{ h1: 0x78039000, h2: 0xcce049c0 }, DHash{ h1: 0x78c39680, h2: 0xcd01e3d6 },
  DHash{ h1: 0x79839d00, h2: 0xcf231dec }, DHash{ h1: 0x79439b80, h2: 0xcec2b7fa },
  DHash{ h1: 0x7b038a00, h2: 0xcb66e198 }, DHash{ h1: 0x7bc38c80, h2: 0xca874b8e },
  DHash{ h1: 0x7a838700, h2: 0xc8a5b5b4 }, DHash{ h1: 0x7a438180, h2: 0xc9441fa2 },
  DHash{ h1: 0x7e03a400, h2: 0xc3ed1970 }, DHash{ h1: 0x7ec3a280, h2: 0xc20cb366 },
  DHash{ h1: 0x7f83a900, h2: 0xc02e4d5c }, DHash{ h1: 0x7f43af80, h2: 0xc1cfe74a },
  DHash{ h1: 0x7d03be00, h2: 0xc46bb128 }, DHash{ h1: 0x7dc3b880, h2: 0xc58a1b3e },
  DHash{ h1: 0x7c83b300, h2: 0xc7a8e504 }, DHash{ h1: 0x7c43b580, h2: 0xc6494f12 },
  DHash{ h1: 0x7403f800, h2: 0xd2fae8a0 }, DHash{ h1: 0x74c3fe80, h2: 0xd31b42b6 },
  DHash{ h1: 0x7583f500, h2: 0xd139bc8c }, DHash{ h1: 0x7543f380, h2: 0xd0d8169a },
  DHash{ h1: 0x7703e200, h2: 0xd57c40f8 }, DHash{ h1: 0x77c3e480, h2: 0xd49deaee },
  DHash{ h1: 0x7683ef00, h2: 0xd6bf14d4 }, DHash{ h1: 0x7643e980, h2: 0xd75ebec2 },
  DHash{ h1: 0x7203cc00, h2: 0xddf7b810 }, DHash{ h1: 0x72c3ca80, h2: 0xdc161206 },
  DHash{ h1: 0x7383c100, h2: 0xde34ec3c }, DHash{ h1: 0x7343c780, h2: 0xdfd5462a },
  DHash{ h1: 0x7103d600, h2: 0xda711048 }, DHash{ h1: 0x71c3d080, h2: 0xdb90ba5e },
  DHash{ h1: 0x7083db00, h2: 0xd9b24464 }, DHash{ h1: 0x7043dd80, h2: 0xd853ee72 },
  DHash{ h1: 0x5002e000, h2: 0x88bf8e80 }, DHash{ h1: 0x50c2e680, h2: 0x895e2496 },
  DHash{ h1: 0x5182ed00, h2: 0x8b7cdaac }, DHash{ h1: 0x5142eb80, h2: 0x8a9d70ba },
  DHash{ h1: 0x5302fa00, h2: 0x8f3926d8 }, DHash{ h1: 0x53c2fc80, h2: 0x8ed88cce },
  DHash{ h1: 0x5282f700, h2: 0x8cfa72f4 }, DHash{ h1: 0x5242f180, h2: 0x8d1bd8e2 },
  DHash{ h1: 0x5602d400, h2: 0x87b2de30 }, DHash{ h1: 0x56c2d280, h2: 0x86537426 },
  DHash{ h1: 0x5782d900, h2: 0x84718a1c }, DHash{ h1: 0x5742df80, h2: 0x8590200a },
  DHash{ h1: 0x5502ce00, h2: 0x80347668 }, DHash{ h1: 0x55c2c880, h2: 0x81d5dc7e },
  DHash{ h1: 0x5482c300, h2: 0x83f72244 }, DHash{ h1: 0x5442c580, h2: 0x82168852 },
  DHash{ h1: 0x5c028800, h2: 0x96a52fe0 }, DHash{ h1: 0x5cc28e80, h2: 0x974485f6 },
  DHash{ h1: 0x5d828500, h2: 0x95667bcc }, DHash{ h1: 0x5d428380, h2: 0x9487d1da },
  DHash{ h1: 0x5f029200, h2: 0x912387b8 }, DHash{ h1: 0x5fc29480, h2: 0x90c22dae },
  DHash{ h1: 0x5e829f00, h2: 0x92e0d394 }, DHash{ h1: 0x5e429980, h2: 0x93017982 },
  DHash{ h1: 0x5a02bc00, h2: 0x99a87f50 }, DHash{ h1: 0x5ac2ba80, h2: 0x9849d546 },
  DHash{ h1: 0x5b82b100, h2: 0x9a6b2b7c }, DHash{ h1: 0x5b42b780, h2: 0x9b8a816a },
  DHash{ h1: 0x5902a600, h2: 0x9e2ed708 }, DHash{ h1: 0x59c2a080, h2: 0x9fcf7d1e },
  DHash{ h1: 0x5882ab00, h2: 0x9ded8324 }, DHash{ h1: 0x5842ad80, h2: 0x9c0c2932 },
  DHash{ h1: 0x48023000, h2: 0xb48acc40 }, DHash{ h1: 0x48c23680, h2: 0xb56b6656 },
  DHash{ h1: 0x49823d00, h2: 0xb749986c }, DHash{ h1: 0x49423b80, h2: 0xb6a8327a },
  DHash{ h1: 0x4b022a00, h2: 0xb30c6418 }, DHash{ h1: 0x4bc22c80, h2: 0xb2edce0e },
  DHash{ h1: 0x4a822700, h2: 0xb0cf3034 }, DHash{ h1: 0x4a422180, h2: 0xb12e9a22 },
  DHash{ h1: 0x4e020400, h2: 0xbb879cf0 }, DHash{ h1: 0x4ec20280, h2: 0xba6636e6 },
  DHash{ h1: 0x4f820900, h2: 0xb844c8dc }, DHash{ h1: 0x4f420f80, h2: 0xb9a562ca },
  DHash{ h1: 0x4d021e00, h2: 0xbc0134a8 }, DHash{ h1: 0x4dc21880, h2: 0xbde09ebe },
  DHash{ h1: 0x4c821300, h2: 0xbfc26084 }, DHash{ h1: 0x4c421580, h2: 0xbe23ca92 },
  DHash{ h1: 0x44025800, h2: 0xaa906d20 }, DHash{ h1: 0x44c25e80, h2: 0xab71c736 },
  DHash{ h1: 0x45825500, h2: 0xa953390c }, DHash{ h1: 0x45425380, h2: 0xa8b2931a },
  DHash{ h1: 0x47024200, h2: 0xad16c578 }, DHash{ h1: 0x47c24480, h2: 0xacf76f6e },
  DHash{ h1: 0x46824f00, h2: 0xaed59154 }, DHash{ h1: 0x46424980, h2: 0xaf343b42 },
  DHash{ h1: 0x42026c00, h2: 0xa59d3d90 }, DHash{ h1: 0x42c26a80, h2: 0xa47c9786 },
  DHash{ h1: 0x43826100, h2: 0xa65e69bc }, DHash{ h1: 0x43426780, h2: 0xa7bfc3aa },
  DHash{ h1: 0x41027600, h2: 0xa21b95c8 }, DHash{ h1: 0x41c27080, h2: 0xa3fa3fde },
  DHash{ h1: 0x40827b00, h2: 0xa1d8c1e4 }, DHash{ h1: 0x40427d80, h2: 0xa0396bf2 },
];

const CRC_HINIT1: u32 = 0xFAC432B1;
const CRC_HINIT2: u32 = 0x0CD5E44A;

impl DHash {
    /// Calculate the Diablo crc64 hash of a bunch of bytes.
    pub fn hash(msgid: &[u8]) -> DHash {
        let mut hv = DHash {
            h1: CRC_HINIT1,
            h2: CRC_HINIT2,
        };
        for b in msgid {
            let i = ((hv.h1 >> 24) & 0xff) as usize;
            hv.h1 = (hv.h1 << 8) ^ (hv.h2 >> 24) ^ CRC_XOR_TABLE[i].h1;
            hv.h2 = (hv.h2 << 8) ^ (*b as u32) ^ CRC_XOR_TABLE[i].h2;
        }
        // Note from the author of the diablo implementation lib/hash.c :
        // Fold the generated CRC.  Note, this is buggy but it is too late
        // for me to change it now.  I should have XOR'd the 1 in, not OR'd
        // it when folding the bits.
        if (hv.h1 & 0x80000000) != 0 {
            hv.h1 = (hv.h1 & 0x7FFFFFFF) | 1;
        }
        if (hv.h2 & 0x80000000) != 0 {
            hv.h2 = (hv.h2 & 0x7FFFFFFF) | 1;
        }
        hv.h1 |= 0x80000000;
        hv
    }

    /// Calculate the Diablo crc64 hash of a string.
    pub fn hash_str(msgid: &str) -> DHash {
        DHash::hash(msgid.as_bytes())
    }

    /// Get the hash as a 61 bit value. 61 bits, because we leave out the
    /// highest bit of h1, and the lower bits of both h1 and h2.
    // Remind me why we do this again?
    pub fn as_u64(&self) -> u64 {
        let h1 = ((self.h1 & 0x7ffffff) >> 1) as u64;
        let h2 = (self.h2 >> 1) as u64;
        (h1 << 31) | h2
    }
}
