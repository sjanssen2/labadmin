from unittest import TestCase, main

from knimin.lib.vioscreen import Vioscreen


class TestVioscreen(TestCase):
    def setUp(self):
        self.usernames = ["11111111111111111", "KLUCBTestuser",
                          "11111111111111112", "8ea4723933145bb0",
                          "1f2daf7176c271a6", "bc2b07307df3b283",
                          "62f4ea819b1ac97b", "193e045d9a1cb3aa",
                          "59f177181c280589", "9bbc04bf3893b80a"]

    def test_main(self):
        v = Vioscreen()
        sessions = []
        for user in self.usernames:
            sessions.extend(v.get_user_sessions(user))
        # print(sessions[0])
        x = {it: v._get_session_information(it, sessions[0])
             for it in v.VALID_INFOTYPES}
        print(x)


if __name__ == "__main__":
    main()
