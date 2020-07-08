import React from 'react';
import styled from 'styled-components'
import * as Shared from './PageComponents.jsx'
import InlineCluster from '@bedrock-layout/inline-cluster'
import { VictoryBar, VictoryChart, VictoryAxis, VictoryStack,
    VictoryTheme, VictoryLabel } from 'victory';




class MeanSpeed extends React.Component {

    constructor(props){
        super(props);
        this.state = {
            data: [{"city":"Aarhus","car_r_s":25.0,"car_m_s":24.7,"bike_s":16.8,"walk_s":5.0},{"city":"Adelaide","car_r_s":27.5,"car_m_s":29.6,"bike_s":17.7,"walk_s":5.0},{"city":"Amsterdam","car_r_s":36.2,"car_m_s":37.2,"bike_s":15.5,"walk_s":4.9},{"city":"Auckland","car_r_s":31.5,"car_m_s":42.2,"bike_s":17.1,"walk_s":5.0},{"city":"Barcelona","car_r_s":46.5,"car_m_s":48.4,"bike_s":16.4,"walk_s":5.0},{"city":"Berlin","car_r_s":27.9,"car_m_s":30.9,"bike_s":17.3,"walk_s":5.0},{"city":"Bern","car_r_s":22.6,"car_m_s":23.1,"bike_s":16.6,"walk_s":5.0},{"city":"Birmingham","car_r_s":28.3,"car_m_s":30.2,"bike_s":17.0,"walk_s":5.0},{"city":"Bogota","car_r_s":43.7,"car_m_s":43.7,"bike_s":17.6,"walk_s":5.0},{"city":"Brussels","car_r_s":30.4,"car_m_s":31.6,"bike_s":16.5,"walk_s":5.0},{"city":"Budapest","car_r_s":26.5,"car_m_s":29.7,"bike_s":16.9,"walk_s":5.0},{"city":"Buenos Aires","car_r_s":33.5,"car_m_s":21.0,"bike_s":15.5,"walk_s":5.0},{"city":"Calgary","car_r_s":43.5,"car_m_s":44.2,"bike_s":17.1,"walk_s":5.0},{"city":"Chicago","car_r_s":39.9,"car_m_s":35.3,"bike_s":17.5,"walk_s":5.0},{"city":"Copenhagen","car_r_s":23.2,"car_m_s":27.0,"bike_s":17.2,"walk_s":5.0},{"city":"Dallas","car_r_s":49.3,"car_m_s":44.2,"bike_s":17.2,"walk_s":5.0},{"city":"Delhi [New Delhi]","car_r_s":16.0,"car_m_s":16.6,"bike_s":17.7,"walk_s":5.0},{"city":"Dublin","car_r_s":13.8,"car_m_s":17.4,"bike_s":17.4,"walk_s":5.0},{"city":"Gothenburg","car_r_s":33.2,"car_m_s":34.9,"bike_s":17.0,"walk_s":5.0},{"city":"Graz","car_r_s":33.3,"car_m_s":34.7,"bike_s":17.5,"walk_s":5.0},{"city":"Hamburg","car_r_s":33.5,"car_m_s":37.0,"bike_s":16.2,"walk_s":5.0},{"city":"Helsinki","car_r_s":28.4,"car_m_s":29.7,"bike_s":16.9,"walk_s":5.0},{"city":"Houston","car_r_s":58.3,"car_m_s":54.4,"bike_s":17.7,"walk_s":5.0},{"city":"Kuala Lumpur","car_r_s":36.6,"car_m_s":40.5,"bike_s":17.2,"walk_s":4.9},{"city":"Lisbon","car_r_s":39.5,"car_m_s":37.6,"bike_s":16.2,"walk_s":5.0},{"city":"London","car_r_s":21.8,"car_m_s":23.1,"bike_s":17.8,"walk_s":5.0},{"city":"Los Angeles","car_r_s":52.7,"car_m_s":52.5,"bike_s":17.6,"walk_s":5.0},{"city":"Luxembourg","car_r_s":26.0,"car_m_s":29.0,"bike_s":16.3,"walk_s":5.0},{"city":"Madrid","car_r_s":31.8,"car_m_s":39.3,"bike_s":17.2,"walk_s":5.0},{"city":"Marseille","car_r_s":46.9,"car_m_s":51.3,"bike_s":17.3,"walk_s":5.0},{"city":"Melbourne","car_r_s":24.9,"car_m_s":29.4,"bike_s":17.7,"walk_s":5.0},{"city":"Mexico City","car_r_s":44.0,"car_m_s":39.7,"bike_s":17.7,"walk_s":5.0},{"city":"Montreal","car_r_s":41.4,"car_m_s":34.5,"bike_s":17.8,"walk_s":5.0},{"city":"Mumbai","car_r_s":18.5,"car_m_s":17.6,"bike_s":17.1,"walk_s":5.0},{"city":"New York","car_r_s":40.4,"car_m_s":31.5,"bike_s":17.0,"walk_s":5.0},{"city":"Oslo","car_r_s":24.5,"car_m_s":28.7,"bike_s":16.7,"walk_s":5.0},{"city":"Paris","car_r_s":39.4,"car_m_s":44.1,"bike_s":16.7,"walk_s":4.9},{"city":"Perth","car_r_s":32.2,"car_m_s":34.4,"bike_s":17.3,"walk_s":5.0},{"city":"Philadelphia","car_r_s":48.2,"car_m_s":40.3,"bike_s":17.4,"walk_s":5.0},{"city":"Phoenix","car_r_s":46.1,"car_m_s":42.8,"bike_s":17.5,"walk_s":5.0},{"city":"Portland","car_r_s":47.1,"car_m_s":46.9,"bike_s":17.9,"walk_s":5.0},{"city":"Prague","car_r_s":35.8,"car_m_s":39.9,"bike_s":16.0,"walk_s":5.0},{"city":"Riga","car_r_s":27.5,"car_m_s":28.3,"bike_s":15.6,"walk_s":5.0},{"city":"Rio de Janeiro","car_r_s":47.2,"car_m_s":31.8,"bike_s":17.2,"walk_s":5.0},{"city":"Salvador","car_r_s":36.5,"car_m_s":22.9,"bike_s":17.3,"walk_s":5.0},{"city":"San Antonio","car_r_s":45.1,"car_m_s":44.0,"bike_s":17.2,"walk_s":5.0},{"city":"Santiago","car_r_s":51.0,"car_m_s":51.0,"bike_s":17.5,"walk_s":5.0},{"city":"Stockholm","car_r_s":44.0,"car_m_s":48.7,"bike_s":17.3,"walk_s":5.0},{"city":"Sydney","car_r_s":31.8,"car_m_s":34.5,"bike_s":17.6,"walk_s":5.0},{"city":"S\u00e3o Paulo","car_r_s":43.3,"car_m_s":29.5,"bike_s":17.6,"walk_s":5.0},{"city":"Tallinn","car_r_s":33.3,"car_m_s":33.5,"bike_s":16.8,"walk_s":5.0},{"city":"Toronto","car_r_s":34.4,"car_m_s":29.7,"bike_s":17.1,"walk_s":5.0},{"city":"Utrecht","car_r_s":35.8,"car_m_s":37.7,"bike_s":16.5,"walk_s":5.0},{"city":"Vienna","car_r_s":37.7,"car_m_s":40.8,"bike_s":16.9,"walk_s":5.0},{"city":"Warsaw","car_r_s":23.8,"car_m_s":30.7,"bike_s":17.0,"walk_s":5.0},{"city":"Wellington","car_r_s":24.8,"car_m_s":33.6,"bike_s":17.1,"walk_s":5.0},{"city":"York","car_r_s":37.4,"car_m_s":37.3,"bike_s":15.9,"walk_s":5.0}],
            mode: 'drive',
            time: 'r'
        };
    }

    translateModeState = state => {
        if(state.mode == 'drive') return 'car_' + state.time + '_s'
        if(state.mode == 'bike') return 'bike_s'
        if(state.mode == 'transit') return 'transit_s'
        if(state.mode == 'walk') return 'walk_s'
    }
  
    render(){
        return (
            <Shared.Page>
                <Shared.Heading2>What's your speed?</Shared.Heading2>
                <Shared.BodyText>Which speed is an average route in your city? This 
                    metric weighs all routes to the center, and compares them by 
                    the population using them. Keep in mind that this includes the whole city! 
                    If your urban center is an old and congested one, you might
                    find it quite low here.
                </Shared.BodyText>

                {/* <VictoryChart
                    domainPadding={20}
                    width={1000}
                    height={700}
                    padding={{ left: 100, right: 100 }}
                    domain={{ y: [0, 60] }}
                >

                    <VictoryStack horizontal={true}>
                        <VictoryBar
                            data={this.state.data}
                            x="city"
                            animate={{
                                duration: 2000,
                                onLoad: { duration: 1000 }
                            }}
                            barWidth={10}
                            sortKey={this.translateModeState(this.state)}
                            sortOrder={'ascending'}
                            style={{
                                data: { fill: "#4B4843" },
                                labels: {fontSize: 10, padding: 15 , width: 60}
                            }}
                            labels={({ datum }) => datum.car_r_s}
                            // labelComponent={<VictoryLabel dy={-30}/>}
                            y={this.translateModeState(this.state)}
                        />
                    </VictoryStack>
                </VictoryChart> */}


            </Shared.Page>
        )
    }
}


export default MeanSpeed