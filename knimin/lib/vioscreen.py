import requests

from knimin.lib.configuration import config


class Vioscreen(object):
    VALID_INFOTYPES = ['detail', 'foodcomponents', 'percentenergy',
                       'mpeds', 'eatingpatterns', 'foodconsumption',
                       'dietaryscore']

    def __init__(self):
        self.bearer_token = None
        self._fetch_token()
        self.headers = {
            'Authorization': "Bearer %s" % self.bearer_token,
            'Accept': "application/json",
        }

    def _fetch_token(self):
        """Retrieves an access token from the Vioscreen server
        Raises
        ------
        ValueError
            If the authentication with the Vioscreen server fails
        """
        data = {'username': config.vioscreen_user,
                'password': config.vioscreen_password}
        r = requests.post("%s/%s/auth/login" % (config.vioscreen_host,
                                                config.vioscreen_regcode),
                          data=data)
        if r.status_code != 200:
            raise ValueError("Can't authenticate with the Vioscreen server")
        self.bearer_token = r.json()['token']

    def get_user_sessions(self, survey_id):
        """ We use survey_id as username """
        r = requests.get('%s/%s/users/%s/sessions' % (config.vioscreen_host,
                                                      config.vioscreen_regcode,
                                                      survey_id),
                         headers=self.headers)
        if r.status_code != 200:
            print(r.reason, r.url)
            raise ValueError("Can't obtain sessions for user %s" % survey_id)
        return [session[u'sessionId'] for session in r.json()[u'sessions']]

    def _get_session_information(self, infotype, session_id):
        if infotype not in self.VALID_INFOTYPES:
            raise KeyError("infotype '%s' is not supported. Choose from '%s'" %
                           (infotype, "', '".join(self.VALID_INFOTYPES)))

        r = requests.get('%s/%s/sessions/%s/%s' %
                         (config.vioscreen_host,
                          config.vioscreen_regcode,
                          session_id,
                          infotype),
                         headers=self.headers)
        if r.status_code != 200:
            raise ValueError("Can't obtain %s for session %s" %
                             (infotype, session_id))
        return r.json()
