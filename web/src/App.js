import React from 'react';
import { Switch, Route } from 'react-router-dom';

import PageMain from './PageMain';
import PageMap from './PageMap';

const App = () => {
  return (
    <div className="App">
      <Switch>
        <Route exact path={process.env.PUBLIC_URL + '/'} component={PageMain}></Route>
        <Route exact path={process.env.PUBLIC_URL + '/map/:city/:townhall/:lat/:lon/'} component={PageMap}></Route>
      </Switch>
    </div>
  );
}

export default App;