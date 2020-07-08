import React from 'react';
import ReactDOM from 'react-dom';
import * as V from 'victory';
import * as Shared from './PageComponents.jsx'

import { VictoryBar, VictoryChart, VictoryAxis, VictoryStack,
    VictoryTheme, VictoryGroup} from 'victory';

export const DetourGraph = (props) => {


    return (
        <Shared.Page>
            <Shared.Heading2>How long is the detour?</Shared.Heading2>
            <Shared.BodyText>
                Find the extra distance a vehicle has to make to get to the center of the city. This can
                give an indication of less accessible parts of the city, especially with public transit. 
                Download the csv-file with data <a href="https://github.com/idegeus/UrbanTransportTimes">here</a>.
            </Shared.BodyText>
        </Shared.Page>
    )

}

export default DetourGraph