<Heading2>Until what distance is the bike faster?</Heading2>
            <BodyText>
              The speed at which a bike and a car move through the city depends 
              on lots of factors, two important ones are city layout and departure 
              time. Here we compare at which route distance the car is on average 
              faster than the bike. The core is until where the bike is faster, the 
              outer ring is where the car is faster. High values can mean a city with 
              a lot of dense traffic or optimized bike facilities. 
            </BodyText>

            <InlineCluster gutter="lg" justify="start" align="start">
              <span>
                Departure time: 
                <select 
                    name='departure_time'
                    onChange={this.handleChange}
                    value={this.state.departure_time}>
                  <option value='r'>Rush Hour (8:00)</option>
                  <option value='m'>Mid-day (12:00)</option>
                </select>
              </span>
              <span>
                Sorted: 
                <select 
                    name='sort_direction'
                    onChange={this.handleSortChange}
                    value={this.state.sort_direction}>
                  <option value='pt_dsc'>Km descending</option>
                  <option value='pt_asc'>Km ascending</option>
                  <option value='city_asc'>City A-Z</option>
                </select>
              </span>
              <span>
                Parking-time car: 
                <select 
                    name='car_p_time'
                    onChange={this.handleChange}
                    value={this.state.car_p_time}>
                  <option value='0'>Zero</option>
                  <option value='180'>2.5 minutes</option>
                  <option value='300'>5 minutes</option>
                  <option value='450'>7.5 minutes</option>
                  <option value='600'>10 minutes</option>
                  <option value='750'>12.5 minutes</option>
                  <option value='900'>15 minutes</option>
                </select>
              </span>
            </InlineCluster>